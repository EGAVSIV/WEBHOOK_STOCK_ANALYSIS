"""
OI Gate Unified Webhook  (TradingView -> Upstox OI check -> Telegram + MTF analysis)
Run:  gunicorn app:app --workers 1 --threads 8 --timeout 60
"""
import gzip
import json
import logging
import math
import os
import re
import threading
import time
from collections import deque
from datetime import datetime

import pytz
import requests
from flask import Flask, Response, jsonify, request

import analysis

# ============================================================
# APP + LOGGING
# ============================================================
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("oi-gate-unified")
IST = pytz.timezone("Asia/Kolkata")

# ============================================================
# CONFIG (environment variables)
# ============================================================
def _flag(name, default):
    return os.environ.get(name, default).lower() in ("1", "true", "yes", "y", "on")


UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()
OI_DROP_THRESHOLD = float(os.environ.get("OI_DROP_THRESHOLD", "-5.0"))
DEFAULT_QUANTITY = os.environ.get("DEFAULT_QUANTITY", "").strip()
OTM_LEVEL = int(os.environ.get("OTM_LEVEL", "1"))
TEST_MODE = _flag("TEST_MODE", "true")
ASYNC_WEBHOOK = _flag("ASYNC_WEBHOOK", "true")      # reply to TradingView instantly (3s limit)
INSTRUMENT_MASTER_PATH = os.environ.get(
    "INSTRUMENT_MASTER_PATH", os.environ.get("UPSTOX_CSV", "NSE.json.gz")).strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")

UPSTOX_QUOTE_URL = "https://api.upstox.com/v2/market-quote/quotes"
UPSTOX_CANDLE_URL = "https://api.upstox.com/v2/historical-candle/intraday"
UPSTOX_ORDER_PLACE_URL = "https://api.upstox.com/v2/order/place"

UPSTOX_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
}

INDEX_SPOT_KEYS = {
    "NIFTY": "NSE_INDEX|Nifty 50",
    "BANKNIFTY": "NSE_INDEX|Nifty Bank",
    "FINNIFTY": "NSE_INDEX|Nifty Fin Service",
    "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
    "SENSEX": "BSE_INDEX|SENSEX",
}
INDEX_STEP_MAP = {"NIFTY": 50, "FINNIFTY": 50, "MIDCPNIFTY": 25, "BANKNIFTY": 100, "SENSEX": 100}

# ============================================================
# GLOBAL STATE
# ============================================================
equity_map, spot_map, options_map, symbol_to_option_map = {}, {}, {}, {}
trace_events = deque(maxlen=2000)
ordered_today = set()
order_date = None
trade_counter = 0
_state_lock = threading.Lock()

stats = {
    "received": 0, "parsed": 0, "oi_data_success": 0, "condition_met": 0, "discarded": 0,
    "test_signals": 0, "orders": 0, "errors": 0, "spot_signals": 0, "option_signals": 0,
    "telegram_sent": 0,
}

# ============================================================
# HELPERS
# ============================================================
def ist_now_str():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")


def clean_symbol(value):
    return str(value or "").strip().strip('"').strip("'").upper()


def reset_order_day():
    global order_date, trade_counter
    today = datetime.now(IST).date()
    if order_date != today:
        ordered_today.clear()
        order_date = today
        trade_counter = 0


def trace(signal_id, symbol, stage, status, message="", detail=None):
    detail = detail or {}
    trace_events.appendleft({
        "time": ist_now_str(), "signal_id": signal_id, "symbol": symbol, "stage": stage,
        "status": status, "message": message,
        "spot_ltp": detail.get("spot_ltp", "N/A"),
        "option_symbol": detail.get("option_symbol", "N/A"),
        "strike": detail.get("strike", "N/A"),
        "option_ltp": detail.get("option_ltp", "N/A"),
        "oi_change_pct": detail.get("oi_change_pct", "N/A"),
        "detail": detail,
    })
    log.info("[%s] [%s] %s | %s | %s", ist_now_str(), signal_id, stage, status, message)


def is_market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return start <= now <= end


# ============================================================
# TELEGRAM
# ============================================================
def _send_telegram_worker(text, bot_token, chat_id):
    if not bot_token or not chat_id:
        log.error("Telegram cancelled: Bot Token or Chat ID missing")
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
               "disable_web_page_preview": True}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            log.info("Telegram notification delivered")
        else:
            log.error("Telegram API error (HTTP %s): %s", resp.status_code, resp.text)
    except Exception as exc:
        log.error("Telegram exception: %s", exc)


def notify_telegram(text):
    token = os.environ.get("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN).strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID).strip()
    stats["telegram_sent"] += 1
    threading.Thread(target=_send_telegram_worker, args=(text, token, chat_id), daemon=True).start()


# ============================================================
# GREEKS + STATUS FLAG
# ============================================================
def calculate_estimated_greeks(spot_ltp, strike, option_type, days_to_expiry=5, iv=0.25):
    try:
        if not spot_ltp or not strike or spot_ltp == "N/A" or strike == "N/A":
            return "N/A", "N/A", "N/A", "N/A"
        spot_ltp, strike = float(spot_ltp), float(strike)
        t = max(days_to_expiry, 1) / 365.0
        r = 0.07
        d1 = (math.log(spot_ltp / strike) + (r + 0.5 * iv ** 2) * t) / (iv * math.sqrt(t))
        cdf = lambda x: 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
        pdf = lambda x: (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x ** 2)
        delta = cdf(d1) if option_type in ("CE", "CALL", "C") else cdf(d1) - 1.0
        vega = (spot_ltp * pdf(d1) * math.sqrt(t)) / 100.0
        theta = -((spot_ltp * pdf(d1) * iv) / (2 * math.sqrt(t))) / 365.0
        return round(delta, 2), round(theta, 2), round(vega, 2), f"{round(iv * 100, 2)}%"
    except Exception:
        return "N/A", "N/A", "N/A", "N/A"


def get_status_flag(opp_decay_val, otm1_decay_val, otm2_decay_val):
    def parse_val(val):
        if val in ("N/A", None, ""):
            return None
        try:
            return float(str(val).replace("%", "").strip())
        except ValueError:
            return None

    opp, otm1, otm2 = parse_val(opp_decay_val), parse_val(otm1_decay_val), parse_val(otm2_decay_val)
    if opp is None:
        return ""
    if opp < 0:
        return "🔴 "
    if opp > 0 and otm1 is not None and otm2 is not None:
        if otm1 < 0 and otm2 < 0:
            return "🟢 "
        if otm1 < 0 and otm2 > 0:
            return "🟠 "
        if otm1 > 0 and otm2 > 0:
            return "🟡 "
    return ""


# ============================================================
# INSTRUMENT MASTER LOADER
# ============================================================
def parse_expiry(raw):
    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromtimestamp(int(raw) / 1000, IST).date()
    except Exception:
        return None


def _first_present(row, *keys):
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return v
    return ""


def load_rows(path):
    if not os.path.exists(path):
        log.warning("Master file missing: %s", path)
        return
    try:
        if path.lower().endswith(".json.gz"):
            with gzip.open(path, "rt", encoding="utf-8") as f:
                data = json.load(f)
        elif path.lower().endswith(".json"):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            log.error("Unsupported master file type: %s", path)
            return
        for row in data:
            yield {str(k): ("" if v is None else str(v)).strip() for k, v in row.items()}
    except Exception as exc:
        log.error("Failed loading master file: %s", exc)


def load_maps():
    global equity_map, spot_map, options_map, symbol_to_option_map
    equity_map, spot_map, options_map, symbol_to_option_map = {}, {}, {}, {}

    for row in load_rows(INSTRUMENT_MASTER_PATH):
        key = str(_first_present(row, "instrument_key")).strip()
        if not key:
            continue
        segment = clean_symbol(_first_present(row, "segment"))
        instrument_type = clean_symbol(_first_present(row, "instrument_type"))
        trading_symbol = clean_symbol(_first_present(row, "trading_symbol", "tradingsymbol"))
        trading_symbol_tv = clean_symbol(_first_present(row, "TV Symbol"))

        if segment == "NSE_EQ" and instrument_type == "EQ" and trading_symbol:
            record = {"instrument_key": key, "trading_symbol": trading_symbol, "segment": segment}
            equity_map[trading_symbol] = record
            spot_map[trading_symbol] = record
            continue

        if segment in ("NSE_FO", "NSE_FNO") and instrument_type in ("CE", "PE"):
            underlying = clean_symbol(_first_present(row, "underlying_symbol", "asset_symbol", "name"))
            if not underlying:
                continue
            try:
                strike = float(_first_present(row, "strike_price", "strike"))
            except (TypeError, ValueError):
                continue
            rec = {
                "expiry": parse_expiry(_first_present(row, "expiry")),
                "strike": strike,
                "option_type": instrument_type,
                "instrument_key": key,
                "tradingsymbol": trading_symbol,
                "lot_size": _first_present(row, "lot_size") or "1",
                "underlying_symbol": underlying,
                "underlying_key": str(_first_present(row, "underlying_key", "asset_key")).strip(),
                "name": underlying,
            }
            options_map.setdefault(underlying, []).append(rec)
            if trading_symbol:
                symbol_to_option_map[trading_symbol] = rec
            if trading_symbol_tv:
                symbol_to_option_map[trading_symbol_tv] = rec

    log.info("Master loaded -> option chains: %d | direct option symbols: %d",
             len(options_map), len(symbol_to_option_map))


load_maps()

# ============================================================
# SYMBOL TYPE IDENTIFICATION
# ============================================================
OPTION_PATTERN_ANY = re.compile(r"\b([A-Z&\-]+)(\d{6})(CE|PE|C|P)(\d+(?:\.\d+)?)\b", re.I)

OPTION_PATTERN_STRIKE_BEFORE_TYPE = re.compile(
    r"\b([A-Z0-9&\-]+?)(\d{2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC))"
    r"(\d+(?:\.\d+)?)(CE|PE|C|P)\b", re.I)

SPOT_SIGNAL_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9&\-]+).*?\b(Cross\s*over|Crossover|Break\s*out|Breakout|BO|"
    r"Cross\s*under|Crossunder|Break\s*down|Breakdown|BD)\b", re.I)

CE_SIGNALS = {"crossover", "cross over", "breakout", "break out", "bo"}
PE_SIGNALS = {"crossunder", "cross under", "breakdown", "break down", "bd"}


def identify_symbol_type(raw_text):
    """Returns (mode, symbol, extra). Modes: OPTION_SYMBOL | SPOT_SIGNAL | UNKNOWN."""
    clean_txt = clean_symbol(raw_text)

    m = OPTION_PATTERN_ANY.search(clean_txt)
    if m:
        underlying, expiry_raw, ot, strike = m.group(1).upper(), m.group(2), m.group(3).upper(), m.group(4)
        ot = {"C": "CE", "P": "PE"}.get(ot, ot)
        log.info("OPTION DETECTED | %s | %s %s %s %s", m.group(0).upper(), underlying, expiry_raw, ot, strike)
        return "OPTION_SYMBOL", m.group(0).upper(), (underlying, expiry_raw, ot, strike)

    m = OPTION_PATTERN_STRIKE_BEFORE_TYPE.search(clean_txt)
    if m:
        underlying, expiry_raw, strike, ot = m.groups()
        ot = {"C": "CE", "P": "PE"}.get(ot.upper(), ot.upper())
        return "OPTION_SYMBOL", m.group(0).upper(), (underlying.upper(), expiry_raw.upper(), ot, strike)

    m = SPOT_SIGNAL_PATTERN.search(raw_text or "")
    if m:
        symbol = clean_symbol(m.group(1))
        sig = re.sub(r"\s+", " ", m.group(2)).strip().lower()
        opt_type = "CE" if sig in CE_SIGNALS else "PE"
        log.info("SPOT SIGNAL | %s | %s", symbol, opt_type)
        return "SPOT_SIGNAL", symbol, opt_type

    log.warning("SYMBOL PARSER UNKNOWN | %s", clean_txt)
    return "UNKNOWN", clean_txt, "UNRECOGNIZED_FORMAT"


# ============================================================
# MARKET DATA
# ============================================================
def get_ltp(instrument_key):
    try:
        res = requests.get(UPSTOX_QUOTE_URL, headers=UPSTOX_HEADERS,
                           params={"instrument_key": instrument_key}, timeout=5)
        if res.status_code == 200:
            payload = res.json().get("data", {})
            if payload:
                quote = list(payload.values())[0]
                return float(quote.get("last_price", 0)), None
        return None, f"HTTP_{res.status_code}"
    except Exception as exc:
        return None, str(exc)


def get_option_candle_data(instrument_key):
    url = f"{UPSTOX_CANDLE_URL}/{instrument_key}/1minute"
    try:
        res = requests.get(url, headers=UPSTOX_HEADERS, timeout=5)
        if res.status_code == 200:
            candles = res.json().get("data", {}).get("candles", [])
            if len(candles) >= 2:
                prev_low = float(candles[-2][3])
                recent_high = float(candles[-1][2])
                return prev_low, round(recent_high * 1.05, 2), float(candles[-1][6])
    except Exception as exc:
        log.warning("Candle fetch failed: %s", exc)
    return "N/A", "N/A", None


def get_upstox_oi_data(instrument_key):
    try:
        res = requests.get(UPSTOX_QUOTE_URL, headers=UPSTOX_HEADERS,
                           params={"instrument_key": instrument_key}, timeout=5)
        if res.status_code != 200:
            return None, f"HTTP_{res.status_code}"
        payload = res.json().get("data", {})
        if not payload:
            return None, "EMPTY_QUOTE_DATA"
        q = list(payload.values())[0]
        cur_oi = float(q.get("oi") or 0)
        ltp = float(q.get("last_price") or 0)
        oi_low = float(q.get("oi_day_low") or 0)
        _, _, open_oi = get_option_candle_data(instrument_key)
        base_oi = open_oi if (open_oi and open_oi > 0) else oi_low
        if base_oi == 0:
            return None, "BASE_OI_IS_ZERO"
        pct = (cur_oi - base_oi) / base_oi * 100.0
        return {"current_oi": cur_oi, "previous_oi": base_oi, "oi_change_pct": pct, "ltp": ltp}, None
    except Exception as exc:
        return None, str(exc)


def select_otm_option(symbol, option_type, ltp):
    chain = options_map.get(symbol, [])
    if not chain:
        return None, f"NO_OPTION_CHAIN_FOR_{symbol}"
    today = datetime.now(IST).date()
    same_type = [c for c in chain if c.get("option_type") == option_type
                 and c.get("expiry") and c["expiry"] >= today]
    if not same_type:
        return None, f"NO_{option_type}_EXPIRIES"
    nearest = min(c["expiry"] for c in same_type)
    contracts = [c for c in same_type if c["expiry"] == nearest]
    if option_type == "CE":
        cands = sorted([c for c in contracts if float(c["strike"]) > ltp], key=lambda c: float(c["strike"]))
    else:
        cands = sorted([c for c in contracts if float(c["strike"]) < ltp],
                       key=lambda c: float(c["strike"]), reverse=True)
    if not cands:
        return None, "NO_OTM_STRIKE_FOUND"
    idx = OTM_LEVEL - 1
    selected = dict(cands[idx if idx < len(cands) else 0])
    selected["expiry_str"] = nearest.strftime("%Y-%m-%d")
    return selected, None


# ============================================================
# OTM + OPPOSITE ANALYSIS
# ============================================================
def _decay_for(chain, strike, opt_type, expiry):
    match = [c for c in chain if float(c.get("strike", 0)) == strike
             and c.get("option_type") == opt_type and c.get("expiry") == expiry]
    if match:
        oi_data, _ = get_upstox_oi_data(match[0]["instrument_key"])
        if oi_data:
            return f"{oi_data['oi_change_pct']:.2f}%", str(oi_data["ltp"])
    return "N/A", "N/A"


def get_otm_symbols_and_decays(underlying, base_strike, opt_type, expiry):
    chain = options_map.get(underlying, [])
    try:
        base_strike = float(base_strike)
    except (TypeError, ValueError):
        return ("N/A",) * 6
    strikes = sorted({float(c["strike"]) for c in chain
                      if c.get("option_type") == opt_type and c.get("expiry") == expiry})
    is_call = opt_type in ("CE", "C")
    if is_call:
        otm = [s for s in strikes if s > base_strike]
    else:
        otm = sorted([s for s in strikes if s < base_strike], reverse=True)
    step = INDEX_STEP_MAP.get(underlying, 20)
    sign = 1 if is_call else -1
    otm1 = otm[0] if len(otm) > 0 else base_strike + sign * step
    otm2 = otm[1] if len(otm) > 1 else base_strike + sign * 2 * step
    d1, l1 = _decay_for(chain, otm1, opt_type, expiry)
    d2, l2 = _decay_for(chain, otm2, opt_type, expiry)
    return otm1, d1, l1, otm2, d2, l2


def get_opposite_decay(underlying, base_strike, current_opt_type, expiry):
    opp = "PE" if current_opt_type in ("CE", "C") else "CE"
    try:
        base_strike = float(base_strike)
    except (TypeError, ValueError):
        return "N/A", "N/A"
    return _decay_for(options_map.get(underlying, []), base_strike, opp, expiry)


# ============================================================
# TELEGRAM MESSAGE FORMATTER
# ============================================================
def format_telegram_message(is_spot_mode, trade_num, symbol, strike, option_type, spot_ltp,
                            option_ltp, oi_change_pct, otm1_stk, otm1_decay, otm1_ltp,
                            otm2_stk, otm2_decay, otm2_ltp, opp_symbol, opp_decay, opp_ltp,
                            alert_time, prev_low="N/A", target="N/A", expiry_str="N/A"):
    flag = get_status_flag(opp_decay, otm1_decay, otm2_decay)
    fmt_oi = f"{oi_change_pct:.2f}%" if isinstance(oi_change_pct, (int, float)) else "N/A"

    if is_spot_mode:
        delta, theta, vega, hv = calculate_estimated_greeks(spot_ltp, strike, option_type)
        opp_t = "PE" if option_type in ("CE", "C") else "CE"
        return (
            f"🚨 <b>New Trade Spot Chart #{trade_num}</b>\n\n"
            f"Alert Time : {alert_time}\nAlert Type : Trendline BO/BD/AAA\n"
            f"Symbol Name : {symbol}\nStrike : {strike}\nOption Type : {option_type}\n"
            f"Expiry : {expiry_str}\nOI Decay : {fmt_oi}\nLTP SPOT : {spot_ltp}\n"
            f"LTP OPTION : {option_ltp}\nDelta : {delta}\nTheta : {theta}\nVEGA : {vega}\nHV : {hv}\n"
            f"SL : LOW of Previous Candle Option Chart ({prev_low})\n"
            f"TARGET : High or NEXT Pivot LEVEL ({target})\n\n"
            f"ℹ️ {flag}<i><b>[JUST FYI]</b> Opposite {strike} {opp_t} OI Decay: <b>{opp_decay}</b>, "
            f"OTM1: {otm1_stk}{option_type} - OI Decay {otm1_decay}, "
            f"OTM2: {otm2_stk}{option_type} - OI Decay {otm2_decay}</i>"
        )
    return (
        f"🚨 <b>New Trade Option Strike Alert #{trade_num}</b>\n\n"
        f"Alert Time : {alert_time}\nAlert Type : EMA 5X50 Closed\nStrike : {symbol}\n"
        f"OI Decay : {fmt_oi}\nLTP : {option_ltp}\n\n"
        f"<b>OTM1</b> : {otm1_stk} | Decay: {otm1_decay} | LTP: {otm1_ltp}\n"
        f"<b>OTM2</b> : {otm2_stk} | Decay: {otm2_decay} | LTP: {otm2_ltp}\n\n"
        f"ℹ️ {flag}<i><b>[JUST FYI]</b> Opposite Strike: <b>{opp_symbol}</b> "
        f"| Opp. OI Decay: <b>{opp_decay}</b> | Opp. LTP: <b>{opp_ltp}</b></i>"
    )


# ============================================================
# ORDER EXECUTION
# ============================================================
def place_upstox_order(instrument_key, quantity, price=0, transaction_type="BUY", is_amo=False):
    body = {
        "quantity": int(quantity),
        "product": "I",
        "validity": "DAY",
        "price": float(price) if is_amo else 0.0,
        "tag": f"oigate{int(time.time())}"[:20],
        "instrument_token": instrument_key,
        "order_type": "LIMIT" if is_amo else "MARKET",
        "transaction_type": transaction_type,
        "disclosed_quantity": 0,
        "trigger_price": 0,
        "is_amo": is_amo,
    }
    return requests.post(UPSTOX_ORDER_PLACE_URL, headers=UPSTOX_HEADERS, json=body, timeout=10)


# ============================================================
# EXACT OPTION CONTRACT FINDER
# ============================================================
def find_option_contract(underlying, expiry_raw, opt_type_raw, strike_raw):
    underlying = clean_symbol(underlying)
    opt_type = "CE" if clean_symbol(opt_type_raw) in ("C", "CE") else "PE"
    try:
        target_strike = float(strike_raw)
    except (TypeError, ValueError):
        return None

    webhook_expiry = None
    exp = str(expiry_raw).upper()
    try:
        if re.fullmatch(r"\d{6}", exp):
            webhook_expiry = datetime.strptime(exp, "%y%m%d").date()
        elif re.fullmatch(r"\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)", exp):
            webhook_expiry = datetime.strptime(f"{exp}{datetime.now(IST).year}", "%d%b%Y").date()
    except Exception:
        pass

    chain = options_map.get(underlying, [])
    log.info("OPTION SEARCH | %s | exp=%s | %s | %s | chain=%d",
             underlying, webhook_expiry, opt_type, target_strike, len(chain))

    for c in chain:
        try:
            if float(c.get("strike", 0)) != target_strike:
                continue
            if clean_symbol(c.get("option_type")) != opt_type:
                continue
            if webhook_expiry is not None and c.get("expiry") != webhook_expiry:
                continue
            log.info("EXACT OPTION FOUND | %s | %s", c.get("tradingsymbol"), c.get("instrument_key"))
            return c
        except Exception:
            continue
    log.warning("OPTION NOT FOUND | %s | %s | %s | %s", underlying, expiry_raw, opt_type, target_strike)
    return None


# ============================================================
# WEBHOOK
# ============================================================
def _resolve_spot(underlying):
    spot = equity_map.get(underlying) or spot_map.get(underlying)
    if not spot and underlying in INDEX_SPOT_KEYS:
        spot = {"instrument_key": INDEX_SPOT_KEYS[underlying]}
    return spot


def process_webhook(raw_payload, signal_id, alert_time):
    """Full signal pipeline. Returns (dict, http_status)."""
    global trade_counter

    mode, parsed_symbol, extra_data = identify_symbol_type(raw_payload)
    log.info("WEBHOOK | %s | MODE=%s | SYMBOL=%s | EXTRA=%s", signal_id, mode, parsed_symbol, extra_data)

    if mode == "UNKNOWN":
        stats["errors"] += 1
        trace(signal_id, raw_payload[:60], "PARSER", "FAILED", extra_data)
        return {"status": "error", "reason": extra_data}, 400

    stats["parsed"] += 1
    stats["option_signals" if mode == "OPTION_SYMBOL" else "spot_signals"] += 1

    option_info = None
    spot = None
    spot_ltp = "N/A"

    # ---------------- OPTION SYMBOL MODE ----------------
    if mode == "OPTION_SYMBOL":
        option_info = symbol_to_option_map.get(parsed_symbol)
        if not option_info and isinstance(extra_data, tuple):
            option_info = find_option_contract(*extra_data)

        if not option_info:
            stats["discarded"] += 1
            trace(signal_id, parsed_symbol, "OPTION_LOOKUP", "FAILED", "Option Symbol not found in Master")
            return {"status": "discarded", "reason": "option_symbol_not_found"}, 200

        underlying = option_info.get("name") or option_info.get("underlying_symbol")
        spot = _resolve_spot(underlying)
        if spot:
            s_ltp, _ = get_ltp(spot["instrument_key"])
            if s_ltp is not None:
                spot_ltp = s_ltp

    # ---------------- SPOT SIGNAL MODE ----------------
    else:
        underlying, opt_type = parsed_symbol, extra_data
        spot = _resolve_spot(underlying)
        if not spot:
            stats["discarded"] += 1
            trace(signal_id, underlying, "SPOT_LOOKUP", "FAILED", "Spot Symbol not in Master")
            return {"status": "discarded", "reason": "spot_not_found"}, 200

        spot_ltp, err = get_ltp(spot["instrument_key"])
        if spot_ltp is None:
            stats["errors"] += 1
            trace(signal_id, underlying, "SPOT_LTP", "FAILED", err)
            return {"status": "error", "reason": "spot_ltp_failed"}, 200

        option_info, err = select_otm_option(underlying, opt_type, spot_ltp)
        if not option_info:
            stats["discarded"] += 1
            trace(signal_id, underlying, "OTM_SELECTION", "FAILED", err)
            return {"status": "discarded", "reason": err}, 200

    # ---------------- MULTI-TIMEFRAME SPOT ANALYSIS ----------------
    signal_tf = analysis.parse_timeframe(raw_payload)
    direction = "BULL" if option_info.get("option_type", "CE") == "CE" else "BEAR"
    spot_key = spot["instrument_key"] if spot else None
    mtf = None
    if spot_key:
        try:
            mtf = analysis.run_mtf_analysis(
                spot_key, signal_tf, direction, UPSTOX_HEADERS,
                meta={
                    "underlying": option_info.get("name") or option_info.get("underlying_symbol"),
                    "option_symbol": option_info.get("tradingsymbol"),
                    "alert_time": alert_time,
                },
            )
            analysis.save_analysis(signal_id, mtf)
        except Exception as exc:
            log.error("MTF analysis failed: %s", exc)

    # ---------------- OI DECAY ANALYSIS ----------------
    oi_data, err = get_upstox_oi_data(option_info["instrument_key"])
    if not oi_data:
        stats["errors"] += 1
        trace(signal_id, parsed_symbol, "OI_FETCH", "FAILED", err,
              detail={"has_analysis": bool(mtf)})
        return {"status": "error", "reason": "oi_fetch_failed"}, 200

    stats["oi_data_success"] += 1
    pct = oi_data["oi_change_pct"]
    option_ltp = oi_data["ltp"]

    log_detail = {
        "spot_ltp": str(spot_ltp),
        "strike": str(option_info.get("strike", "N/A")),
        "option_symbol": str(option_info.get("tradingsymbol", "N/A")),
        "option_ltp": str(option_ltp),
        "oi_change_pct": f"{pct:.2f}%",
        "underlying": str(option_info.get("name") or option_info.get("underlying_symbol", "")),
        "opt_type": str(option_info.get("option_type", "")),
        "expiry": str(option_info.get("expiry") or ""),
        "instrument_key": str(option_info.get("instrument_key", "")),
        "lot_size": str(option_info.get("lot_size", "1")),
        "tradingsymbol": str(option_info.get("tradingsymbol", "")),
        "has_analysis": bool(mtf),
    }

    # ---------------- OI THRESHOLD ----------------
    if pct > OI_DROP_THRESHOLD:
        stats["discarded"] += 1
        trace(signal_id, parsed_symbol, "OI_CHECK", "DISCARDED",
              f"OI Change {pct:.2f}% didn't meet threshold {OI_DROP_THRESHOLD}%", detail=log_detail)
        return {"status": "discarded", "reason": "oi_threshold_not_met"}, 200

    stats["condition_met"] += 1
    with _state_lock:
        trade_counter += 1
        trade_num = trade_counter

    underlying = option_info.get("name") or option_info.get("underlying_symbol", "N/A")
    base_strike = option_info.get("strike", "N/A")
    opt_type = option_info.get("option_type", "CE")
    expiry = option_info.get("expiry")

    otm1_stk, otm1_decay, otm1_ltp, otm2_stk, otm2_decay, otm2_ltp = get_otm_symbols_and_decays(
        underlying, base_strike, opt_type, expiry)
    opp_decay, opp_ltp = get_opposite_decay(underlying, base_strike, opt_type, expiry)
    prev_low, target, _ = get_option_candle_data(option_info["instrument_key"])

    tg_text = format_telegram_message(
        is_spot_mode=(mode == "SPOT_SIGNAL"), trade_num=trade_num, symbol=parsed_symbol,
        strike=base_strike, option_type=opt_type, spot_ltp=spot_ltp, option_ltp=option_ltp,
        oi_change_pct=pct, otm1_stk=otm1_stk, otm1_decay=otm1_decay, otm1_ltp=otm1_ltp,
        otm2_stk=otm2_stk, otm2_decay=otm2_decay, otm2_ltp=otm2_ltp,
        opp_symbol=f"{base_strike}{'PE' if opt_type in ('CE', 'C') else 'CE'}",
        opp_decay=opp_decay, opp_ltp=opp_ltp, alert_time=alert_time,
        prev_low=prev_low, target=target,
        expiry_str=option_info.get("expiry_str", str(expiry or "N/A")),
    )
    link = f"{PUBLIC_BASE_URL}/analysis/{signal_id}" if PUBLIC_BASE_URL else None
    tg_text += "\n" + analysis.telegram_block(mtf, link)
    notify_telegram(tg_text)

    # ---------------- ORDER ----------------
    quantity = DEFAULT_QUANTITY or option_info.get("lot_size", "1")
    tradingsymbol = option_info["tradingsymbol"]
    is_amo = not is_market_open()

    if TEST_MODE:
        stats["test_signals"] += 1
        trace(signal_id, parsed_symbol, "ORDER", "TEST_MODE",
              f"Simulated order for {tradingsymbol} x {quantity}", detail=log_detail)
        return {"status": "test_signal_simulated", "symbol": tradingsymbol}, 200

    if tradingsymbol in ordered_today:
        stats["discarded"] += 1
        trace(signal_id, parsed_symbol, "ORDER", "BLOCKED", "Duplicate signal today", detail=log_detail)
        return {"status": "discarded", "reason": "duplicate"}, 200

    resp = place_upstox_order(option_info["instrument_key"], quantity, price=option_ltp,
                              transaction_type="BUY", is_amo=is_amo)
    if resp.status_code in (200, 201):
        ordered_today.add(tradingsymbol)
        stats["orders"] += 1
        trace(signal_id, parsed_symbol, "ORDER", "SUCCESS", f"Placed for {tradingsymbol}", detail=log_detail)
        return {"status": "order_placed", "response": resp.json()}, 200

    stats["errors"] += 1
    trace(signal_id, parsed_symbol, "ORDER", "FAILED", resp.text, detail=log_detail)
    return {"status": "error", "reason": "order_execution_failed"}, 500


def _safe_process(raw_payload, signal_id, alert_time):
    try:
        process_webhook(raw_payload, signal_id, alert_time)
    except Exception as exc:
        stats["errors"] += 1
        log.exception("Webhook processing crashed")
        trace(signal_id, raw_payload[:60], "PIPELINE", "FAILED", str(exc))


@app.route("/webhook", methods=["POST"])
def webhook():
    if WEBHOOK_SECRET and request.args.get("key") != WEBHOOK_SECRET:
        return jsonify({"status": "error", "reason": "unauthorized"}), 401

    reset_order_day()
    stats["received"] += 1
    signal_id = f"SIG-{int(time.time() * 1000)}"
    alert_time = ist_now_str()
    raw_payload = request.get_data(as_text=True)

    if ASYNC_WEBHOOK:
        threading.Thread(target=_safe_process, args=(raw_payload, signal_id, alert_time),
                         daemon=True).start()
        return jsonify({"status": "accepted", "signal_id": signal_id}), 200

    result, code = process_webhook(raw_payload, signal_id, alert_time)
    return jsonify(result), code


# ============================================================
# API
# ============================================================
@app.route("/api/logs")
def api_logs():
    return jsonify(list(trace_events))


@app.route("/api/stats")
def api_stats():
    return jsonify(stats)


@app.route("/health")
def health():
    return jsonify({"status": "ok", "options_loaded": len(options_map), "time": ist_now_str()})


@app.route("/api/analysis/<signal_id>")
def api_analysis(signal_id):
    if request.args.get("refresh"):
        data = analysis.refresh_analysis(signal_id, UPSTOX_HEADERS)
    else:
        data = analysis.get_analysis(signal_id)
    if not data:
        return jsonify({"status": "error", "reason": "analysis_not_found"}), 404
    return jsonify(data)


@app.route("/analysis/<signal_id>")
def analysis_page(signal_id):
    return Response(analysis.ANALYSIS_HTML, mimetype="text/html")


@app.route("/api/details/<signal_id>")
def api_signal_details(signal_id):
    item = next((ev for ev in trace_events
                 if ev.get("signal_id") == signal_id and (ev.get("detail") or {}).get("instrument_key")), None)
    if not item:
        return jsonify({"status": "error", "reason": "signal_not_found"}), 404

    detail = item["detail"]
    instrument_key = detail["instrument_key"]
    underlying = detail.get("underlying", "")
    opt_type = detail.get("opt_type", "CE")
    lot_size = detail.get("lot_size", "1")
    tradingsymbol = detail.get("tradingsymbol", "")
    try:
        base_strike = float(detail.get("strike", 0))
    except (TypeError, ValueError):
        base_strike = 0.0
    expiry = parse_expiry(detail.get("expiry", ""))

    oi_data, _ = get_upstox_oi_data(instrument_key)
    option_ltp = oi_data["ltp"] if oi_data else "N/A"
    option_oi_pct = f"{oi_data['oi_change_pct']:.2f}%" if oi_data else "N/A"

    spot_ltp = "N/A"
    spot = _resolve_spot(underlying)
    if spot:
        s_ltp, _ = get_ltp(spot["instrument_key"])
        if s_ltp is not None:
            spot_ltp = s_ltp

    otm1_stk, otm1_decay, _, otm2_stk, otm2_decay, _ = get_otm_symbols_and_decays(
        underlying, base_strike, opt_type, expiry)
    opp_decay, _ = get_opposite_decay(underlying, base_strike, opt_type, expiry)
    opp_type = "PE" if opt_type in ("CE", "C") else "CE"

    try:
        estimated_cost = round(float(lot_size) * float(option_ltp), 2)
    except (TypeError, ValueError):
        estimated_cost = "N/A"

    return jsonify({
        "status": "ok", "signal_id": signal_id,
        "symbol": tradingsymbol or item.get("symbol", "N/A"),
        "underlying": underlying, "base_strike": base_strike, "opt_type": opt_type,
        "spot_ltp": spot_ltp, "option_ltp": option_ltp, "option_oi_change_pct": option_oi_pct,
        "lot_size": lot_size, "estimated_cost": estimated_cost,
        "otm1_strike": otm1_stk, "otm1_decay": otm1_decay,
        "otm2_strike": otm2_stk, "otm2_decay": otm2_decay,
        "opposite_type": opp_type, "opposite_strike": base_strike, "opposite_decay": opp_decay,
    })


# ============================================================
# DASHBOARD
# ============================================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>⚡ Unified OI Strategy Terminal</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700;900&family=Rajdhani:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bull:#00e676;--bear:#ff3b5c;--gold:#ffd54a;--cyan:#22e5ff;--purple:#a06bff;--panel:rgba(15,20,35,.72);--panel-brd:rgba(120,140,255,.18);}
*{box-sizing:border-box;}
body{margin:0;font-family:'Rajdhani',sans-serif;color:#e7ecff;min-height:100vh;
background:radial-gradient(1100px 700px at 8% -10%,rgba(160,107,255,.28),transparent 60%),radial-gradient(1000px 650px at 105% 0%,rgba(34,229,255,.22),transparent 55%),radial-gradient(900px 900px at 50% 120%,rgba(0,230,118,.18),transparent 55%),linear-gradient(180deg,#05060d,#0a0d1a 45%,#05060d);background-attachment:fixed;overflow-x:hidden;}
.bg-grid{position:fixed;inset:0;z-index:-3;background-image:linear-gradient(rgba(120,140,255,.06) 1px,transparent 1px),linear-gradient(90deg,rgba(120,140,255,.06) 1px,transparent 1px);background-size:42px 42px;animation:gridDrift 30s linear infinite;mask-image:radial-gradient(circle at 50% 20%,black,transparent 85%);}
@keyframes gridDrift{from{background-position:0 0,0 0;}to{background-position:400px 400px,400px 400px;}}
.floaters{position:fixed;inset:0;z-index:-1;pointer-events:none;overflow:hidden;}
.floaters span{position:absolute;bottom:-10%;opacity:.10;animation:rise linear infinite;}
@keyframes rise{0%{transform:translateY(0) rotate(0);opacity:0;}10%{opacity:.14;}90%{opacity:.10;}100%{transform:translateY(-115vh) rotate(20deg);opacity:0;}}
.topbar{display:flex;align-items:center;justify-content:space-between;padding:18px 26px;margin-bottom:22px;background:linear-gradient(120deg,rgba(160,107,255,.14),rgba(34,229,255,.08));border:1px solid var(--panel-brd);border-radius:18px;backdrop-filter:blur(10px);box-shadow:0 10px 40px rgba(0,0,0,.35);}
.brand{font-family:'Orbitron',sans-serif;font-weight:900;font-size:1.5rem;background:linear-gradient(90deg,var(--cyan),var(--purple) 50%,var(--bull));-webkit-background-clip:text;background-clip:text;color:transparent;}
.brand small{display:block;font-family:'Rajdhani';font-weight:600;color:#9aa4c7;font-size:.78rem;letter-spacing:2px;}
.live-pill{display:flex;align-items:center;gap:8px;background:rgba(0,230,118,.12);border:1px solid rgba(0,230,118,.45);color:var(--bull);padding:7px 16px;border-radius:999px;font-weight:700;font-size:.85rem;}
.live-dot{width:9px;height:9px;border-radius:50%;background:var(--bull);animation:pulse 1.4s infinite;}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(0,230,118,.6);}70%{box-shadow:0 0 0 9px rgba(0,230,118,0);}100%{box-shadow:0 0 0 0 rgba(0,230,118,0);}}
.container-fluid{max-width:1500px;margin:0 auto;padding:22px 22px 40px;}
.stat-grid{display:grid;grid-template-columns:repeat(8,1fr);gap:14px;margin-bottom:24px;}
@media(max-width:1400px){.stat-grid{grid-template-columns:repeat(4,1fr);}}
@media(max-width:700px){.stat-grid{grid-template-columns:repeat(2,1fr);}}
.stat-card{position:relative;overflow:hidden;background:var(--panel);border:1px solid var(--panel-brd);border-radius:16px;padding:16px 14px;backdrop-filter:blur(12px);transition:transform .25s,box-shadow .25s;}
.stat-card:hover{transform:translateY(-4px);box-shadow:0 14px 34px rgba(34,229,255,.15);}
.stat-card .icon{font-size:1.5rem;margin-bottom:6px;}
.stat-card .label{font-size:.72rem;letter-spacing:1.2px;text-transform:uppercase;color:#9aa4c7;font-weight:600;}
.stat-card .value{font-family:'Orbitron',sans-serif;font-size:1.7rem;font-weight:700;margin-top:2px;}
.panel{background:var(--panel);border:1px solid var(--panel-brd);border-radius:18px;padding:20px;backdrop-filter:blur(12px);box-shadow:0 10px 40px rgba(0,0,0,.35);}
.panel-head{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:16px;}
.panel-title{font-family:'Orbitron',sans-serif;font-size:1.05rem;font-weight:700;color:#fff;}
.search-wrap{position:relative;width:290px;max-width:60vw;}
.search-wrap input{width:100%;background:rgba(255,255,255,.05);border:1px solid var(--panel-brd);color:#fff;padding:9px 14px 9px 36px;border-radius:10px;font-size:.9rem;outline:none;}
.search-wrap input:focus{border-color:var(--cyan);box-shadow:0 0 0 3px rgba(34,229,255,.15);}
.search-wrap .ico{position:absolute;left:12px;top:50%;transform:translateY(-50%);}
.table-wrap{max-height:560px;overflow:auto;border-radius:12px;}
table{width:100%;border-collapse:separate;border-spacing:0;font-size:.86rem;}
thead th{position:sticky;top:0;z-index:2;background:rgba(20,25,45,.98);color:#9aa4c7;text-transform:uppercase;letter-spacing:.8px;font-size:.72rem;padding:12px 10px;border-bottom:1px solid var(--panel-brd);white-space:nowrap;text-align:left;}
tbody td{padding:10px;border-bottom:1px solid rgba(120,140,255,.08);white-space:nowrap;color:#dbe1fb;}
tbody tr:hover{background:rgba(120,140,255,.07);}
tbody tr.row-success{box-shadow:inset 3px 0 0 var(--bull);}
tbody tr.row-failed{box-shadow:inset 3px 0 0 var(--bear);}
tbody tr.row-test{box-shadow:inset 3px 0 0 var(--gold);}
.badge-pill{padding:5px 11px;border-radius:999px;font-weight:700;font-size:.72rem;}
.badge-ok{background:rgba(0,230,118,.15);color:var(--bull);border:1px solid rgba(0,230,118,.4);}
.badge-bad{background:rgba(255,59,92,.15);color:var(--bear);border:1px solid rgba(255,59,92,.4);}
.badge-warn{background:rgba(255,213,74,.15);color:var(--gold);border:1px solid rgba(255,213,74,.4);}
.symbol-tag{font-weight:700;color:#fff;}
.symbol-tag.clickable{cursor:pointer;text-decoration:underline dotted rgba(255,255,255,.35);text-underline-offset:4px;}
.empty-state{text-align:center;padding:50px 10px;color:#7c86a8;}
.footer-note{text-align:center;color:#6a749a;font-size:.78rem;margin-top:18px;}
.details-btn{background:linear-gradient(120deg,rgba(34,229,255,.18),rgba(160,107,255,.18));border:1px solid rgba(34,229,255,.4);color:#dff6ff;font-weight:700;font-size:.72rem;padding:6px 12px;border-radius:8px;cursor:pointer;white-space:nowrap;}
.details-btn:hover{border-color:var(--cyan);}
.details-btn:disabled{opacity:.3;cursor:not-allowed;}
.oi-modal-backdrop{position:fixed;inset:0;z-index:1000;display:none;align-items:center;justify-content:center;background:rgba(3,5,12,.72);backdrop-filter:blur(6px);}
.oi-modal-backdrop.show{display:flex;}
.oi-modal{width:min(560px,92vw);max-height:86vh;overflow:auto;background:linear-gradient(160deg,rgba(18,22,42,.97),rgba(10,13,26,.97));border:1px solid var(--panel-brd);border-radius:18px;}
.oi-modal-head{display:flex;align-items:center;justify-content:space-between;padding:18px 20px;border-bottom:1px solid var(--panel-brd);}
.oi-modal-title{display:flex;align-items:center;gap:12px;font-family:'Orbitron',sans-serif;font-weight:700;font-size:1.05rem;color:#fff;}
.oi-close-btn{background:rgba(255,255,255,.06);border:1px solid var(--panel-brd);color:#dbe1fb;width:32px;height:32px;border-radius:9px;cursor:pointer;}
.oi-modal-body{padding:20px;}
.oi-loading,.oi-error{text-align:center;padding:30px 10px;color:#9aa4c7;}
.oi-error{color:var(--bear);}
.oi-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;}
.oi-cell{background:rgba(255,255,255,.04);border:1px solid var(--panel-brd);border-radius:12px;padding:12px 14px;}
.oi-cell .oi-label{font-size:.7rem;letter-spacing:1px;text-transform:uppercase;color:#9aa4c7;font-weight:600;margin-bottom:4px;}
.oi-cell .oi-value{font-family:'Orbitron',sans-serif;font-size:1.15rem;font-weight:700;color:#fff;}
.oi-cell .oi-sub{font-size:.75rem;color:#7c86a8;margin-top:2px;}
.oi-cell.wide{grid-column:1 / -1;}
.oi-pos{color:var(--bull) !important;}.oi-neg{color:var(--bear) !important;}
.oi-cost{border-color:rgba(255,213,74,.35);}.oi-cost .oi-value{color:var(--gold);}
</style></head><body>
<div class="bg-grid"></div><div class="floaters" id="floaters"></div>
<div class="container-fluid">
  <div class="topbar">
    <div class="brand">⚡ UNIFIED OI STRATEGY TERMINAL<small>Signal • OI Decay • Multi-timeframe Analysis • Execution</small></div>
    <div class="live-pill"><span class="live-dot"></span> LIVE</div>
  </div>
  <div class="stat-grid">
    <div class="stat-card"><div class="icon">📡</div><div class="label">Received</div><div class="value" id="s_received">0</div></div>
    <div class="stat-card"><div class="icon">🧩</div><div class="label">Parsed</div><div class="value" id="s_parsed">0</div></div>
    <div class="stat-card"><div class="icon">🐂</div><div class="label">Spot Signals</div><div class="value" id="s_spot">0</div></div>
    <div class="stat-card"><div class="icon">🐻</div><div class="label">Option Strike Signals</div><div class="value" id="s_option">0</div></div>
    <div class="stat-card"><div class="icon">✈️</div><div class="label">Telegram Sent</div><div class="value" id="s_telegram">0</div></div>
    <div class="stat-card"><div class="icon">🎯</div><div class="label">Condition Met</div><div class="value" id="s_condition">0</div></div>
    <div class="stat-card"><div class="icon">✅</div><div class="label">Orders Placed</div><div class="value" id="s_orders">0</div></div>
    <div class="stat-card"><div class="icon">⚠️</div><div class="label">Errors</div><div class="value" id="s_errors">0</div></div>
  </div>
  <div class="panel">
    <div class="panel-head">
      <div class="panel-title">🐂 Signal &amp; Execution Log 🐻</div>
      <div class="search-wrap"><span class="ico">🔍</span><input id="searchBox" type="text" placeholder="Search by symbol name..."></div>
    </div>
    <div class="table-wrap"><table>
      <thead><tr><th>Time</th><th>Signal ID</th><th>Symbol</th><th>Spot LTP</th><th>Strike</th><th>Option Symbol</th><th>Option LTP</th><th>OI Change %</th><th>Stage</th><th>Status</th><th>Message</th><th>Details</th><th>Analysis</th></tr></thead>
      <tbody id="logs"><tr><td colspan="13" class="empty-state">📊 Awaiting activity...</td></tr></tbody>
    </table></div>
  </div>
  <div class="footer-note">Auto-refreshing every 3s • Unified OI Gate Terminal</div>
</div>

<div id="oiModalBackdrop" class="oi-modal-backdrop"><div class="oi-modal">
  <div class="oi-modal-head"><div class="oi-modal-title"><span id="oiSymbolName">—</span><span class="live-pill"><span class="live-dot"></span> LIVE</span></div>
  <button class="oi-close-btn" id="oiCloseBtn">✕</button></div>
  <div class="oi-modal-body" id="oiModalBody"><div class="oi-loading">⏳ Fetching live OI stats...</div></div>
</div></div>

<script>
let allLogs=[];
(function(){const box=document.getElementById('floaters'),g=['🐂','🐻','📈','📉','💹'];
for(let i=0;i<18;i++){const el=document.createElement('span');el.textContent=g[Math.floor(Math.random()*g.length)];
el.style.left=(Math.random()*100)+'vw';el.style.fontSize=(20+Math.random()*30)+'px';
el.style.animationDuration=(14+Math.random()*16)+'s';el.style.animationDelay=(Math.random()*14)+'s';box.appendChild(el);}})();
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
function badgeFor(s){if(s==='SUCCESS'||s==='TEST_MODE')return 'badge-ok';if(s==='DISCARDED'||s==='BLOCKED')return 'badge-warn';return 'badge-bad';}
function rowClassFor(s){if(s==='SUCCESS')return 'row-success';if(s==='TEST_MODE')return 'row-test';if(s==='FAILED')return 'row-failed';return '';}
function renderTable(rows){
  const tbody=document.getElementById('logs');
  if(!rows.length){tbody.innerHTML='<tr><td colspan="13" class="empty-state">🔍 No matching signals found...</td></tr>';return;}
  tbody.innerHTML=rows.map(r=>{
    const live=!!(r.detail&&r.detail.instrument_key), an=!!(r.detail&&r.detail.has_analysis), sid=esc(r.signal_id);
    return `<tr class="${rowClassFor(r.status)}"><td>${esc(r.time)}</td><td>${sid}</td>
    <td><span class="symbol-tag ${live?'clickable':''}" ${live?`onclick="openOiModal('${sid}')"`:''}>${esc(r.symbol)}</span></td>
    <td>${esc(r.spot_ltp)}</td><td>${esc(r.strike)}</td><td>${esc(r.option_symbol)}</td><td>${esc(r.option_ltp)}</td><td>${esc(r.oi_change_pct)}</td>
    <td>${esc(r.stage)}</td><td><span class="badge-pill ${badgeFor(r.status)}">${esc(r.status)}</span></td><td>${esc(r.message)}</td>
    <td><button class="details-btn" ${live?'':'disabled'} onclick="openOiModal('${sid}')">🔎 View OI</button></td>
    <td><button class="details-btn" ${an?'':'disabled'} onclick="window.open('/analysis/${sid}','an_${sid}','width=1280,height=900')">📊 Analysis</button></td></tr>`;
  }).join('');
}
let oiPollTimer=null,oiActive=null;
function fmtPct(v){if(v===null||v===undefined||v==='N/A')return '<span class="oi-value">N/A</span>';const n=parseFloat(String(v).replace('%',''));return `<span class="oi-value ${!isNaN(n)?(n<0?'oi-neg':'oi-pos'):''}">${esc(v)}</span>`;}
function renderOiBody(d){document.getElementById('oiModalBody').innerHTML=`<div class="oi-grid">
<div class="oi-cell wide"><div class="oi-label">Symbol</div><div class="oi-value">${esc(d.symbol||'N/A')}</div><div class="oi-sub">${esc(d.underlying||'')} • Strike ${esc(d.base_strike)} ${esc(d.opt_type||'')}</div></div>
<div class="oi-cell"><div class="oi-label">OTM1 (${esc(d.otm1_strike??'N/A')})</div>${fmtPct(d.otm1_decay)}<div class="oi-sub">OI Decay</div></div>
<div class="oi-cell"><div class="oi-label">OTM2 (${esc(d.otm2_strike??'N/A')})</div>${fmtPct(d.otm2_decay)}<div class="oi-sub">OI Decay</div></div>
<div class="oi-cell wide"><div class="oi-label">Opposite Side (${esc(d.opposite_strike??'N/A')} ${esc(d.opposite_type||'')})</div>${fmtPct(d.opposite_decay)}<div class="oi-sub">OI Decay</div></div>
<div class="oi-cell"><div class="oi-label">Spot LTP</div><div class="oi-value">${esc(d.spot_ltp??'N/A')}</div></div>
<div class="oi-cell"><div class="oi-label">Option LTP</div><div class="oi-value">${esc(d.option_ltp??'N/A')}</div><div class="oi-sub">OI Decay: ${esc(d.option_oi_change_pct??'N/A')}</div></div>
<div class="oi-cell"><div class="oi-label">Lot Size</div><div class="oi-value">${esc(d.lot_size??'N/A')}</div></div>
<div class="oi-cell oi-cost"><div class="oi-label">Estimated Cost</div><div class="oi-value">₹ ${esc(d.estimated_cost??'N/A')}</div><div class="oi-sub">Lot Size × Option LTP</div></div></div>`;}
async function fetchOi(id){try{const d=await(await fetch('/api/details/'+encodeURIComponent(id))).json();
if(d.status!=='ok'){document.getElementById('oiModalBody').innerHTML=`<div class="oi-error">⚠️ ${esc(d.reason||'Unable to load live details')}</div>`;return;}
document.getElementById('oiSymbolName').textContent=d.symbol||'N/A';renderOiBody(d);}
catch(e){document.getElementById('oiModalBody').innerHTML='<div class="oi-error">⚠️ Failed to fetch live OI data</div>';}}
function openOiModal(id){oiActive=id;document.getElementById('oiSymbolName').textContent='—';
document.getElementById('oiModalBody').innerHTML='<div class="oi-loading">⏳ Fetching live OI stats...</div>';
document.getElementById('oiModalBackdrop').classList.add('show');fetchOi(id);
if(oiPollTimer)clearInterval(oiPollTimer);oiPollTimer=setInterval(()=>fetchOi(oiActive),3000);}
function closeOiModal(){document.getElementById('oiModalBackdrop').classList.remove('show');if(oiPollTimer){clearInterval(oiPollTimer);oiPollTimer=null;}oiActive=null;}
document.getElementById('oiCloseBtn').addEventListener('click',closeOiModal);
document.getElementById('oiModalBackdrop').addEventListener('click',e=>{if(e.target.id==='oiModalBackdrop')closeOiModal();});
function applyFilter(){const q=document.getElementById('searchBox').value.trim().toUpperCase();
renderTable(!q?allLogs:allLogs.filter(r=>(r.symbol||'').toUpperCase().includes(q)||(r.option_symbol||'').toUpperCase().includes(q)));}
document.getElementById('searchBox').addEventListener('input',applyFilter);
async function loadLogs(){try{allLogs=await(await fetch('/api/logs')).json();applyFilter();}catch(e){}}
async function loadStats(){try{const d=await(await fetch('/api/stats')).json();
const m={s_received:'received',s_parsed:'parsed',s_spot:'spot_signals',s_option:'option_signals',s_telegram:'telegram_sent',s_condition:'condition_met',s_orders:'orders',s_errors:'errors'};
for(const k in m)document.getElementById(k).textContent=d[m[k]]??0;}catch(e){}}
function refreshAll(){loadLogs();loadStats();}
setInterval(refreshAll,3000);refreshAll();
</script></body></html>
"""


@app.route("/")
def dashboard():
    return Response(DASHBOARD_HTML, mimetype="text/html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
