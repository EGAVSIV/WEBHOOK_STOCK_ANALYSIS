"""
analysis.py  -  Multi-timeframe technical analysis for the OI Gate webhook app.

For every signal it analyses the SPOT chart on:
    signal timeframe  +  2 higher timeframes (HTF)
and returns EMA20/50 state, price vs EMA, MACD bias + tick, RSI, divergences.
"""
import re
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from urllib.parse import quote

import pytz
import requests

IST = pytz.timezone("Asia/Kolkata")

# ------------------------------------------------------------------
# Timeframe config
# ------------------------------------------------------------------
# signal TF  ->  (HTF1, HTF2)
HTF_MAP = {
    "1": ("5", "15"),
    "3": ("15", "60"),
    "5": ("15", "60"),
    "15": ("60", "240"),
    "30": ("60", "240"),
    "60": ("240", "D"),
    "240": ("D", "W"),
    "D": ("W", "M"),
    "W": ("M",),
    "M": (),
}

# tf -> (upstox unit, interval, lookback days)
TF_SPEC = {
    "1": ("minutes", 1, 5),
    "3": ("minutes", 3, 14),
    "5": ("minutes", 5, 28),
    "15": ("minutes", 15, 28),
    "30": ("minutes", 30, 80),
    "60": ("hours", 1, 85),
    "240": ("hours", 4, 85),
    "D": ("days", 1, 400),
    "W": ("weeks", 1, 1500),
    "M": ("months", 1, 3000),
}

TF_LABEL = {"1": "1m", "3": "3m", "5": "5m", "15": "15m", "30": "30m",
            "60": "1H", "240": "4H", "D": "1D", "W": "1W", "M": "1M"}

V3_BASE = "https://api.upstox.com/v3/historical-candle"


def normalize_tf(raw):
    x = str(raw or "").strip().upper()
    m = re.fullmatch(r"(\d+)([HDWM]?)", x)
    if m:
        n, suf = int(m.group(1)), m.group(2)
        if suf == "":
            x = str(n)
        elif suf == "H":
            x = str(n * 60)
        elif suf == "D":
            x = "D"
        elif suf == "W":
            x = "W"
        elif suf == "M":
            x = "M" if n == 1 else str(n)   # TradingView: "1M" = month
    elif x in ("D", "W", "M"):
        pass
    return x if x in TF_SPEC else "15"


def parse_timeframe(raw_text):
    """Reads  TF=15  /  TF:60  /  TF=D  from the alert text."""
    m = re.search(r"\bTF\s*[=:]\s*([0-9]+[A-Za-z]?|[A-Za-z])\b", raw_text or "", re.I)
    return normalize_tf(m.group(1)) if m else "15"


# ------------------------------------------------------------------
# Candle fetch
# ------------------------------------------------------------------
def fetch_candles(instrument_key, tf, headers):
    unit, interval, days = TF_SPEC[tf]
    to_d = datetime.now(IST).date()
    fr = to_d - timedelta(days=days)
    ek = quote(instrument_key, safe="")
    urls = [f"{V3_BASE}/{ek}/{unit}/{interval}/{to_d}/{fr}"]
    if unit in ("minutes", "hours"):                 # add today's live candles
        urls.append(f"{V3_BASE}/intraday/{ek}/{unit}/{interval}")

    rows = {}
    for u in urls:
        try:
            r = requests.get(u, headers=headers, timeout=6)
            if r.status_code == 200:
                for c in (r.json().get("data") or {}).get("candles", []):
                    rows[c[0]] = c
        except Exception:
            continue
    return sorted(rows.values(), key=lambda c: c[0])   # oldest -> newest


# ------------------------------------------------------------------
# Indicators (pure python)
# ------------------------------------------------------------------
def ema(vals, n):
    out, e, k = [], None, 2.0 / (n + 1)
    for i, v in enumerate(vals):
        if i < n - 1:
            out.append(None)
            continue
        e = sum(vals[:n]) / n if e is None else v * k + e * (1 - k)
        out.append(e)
    return out


def macd(closes, fast=12, slow=26, sig=9):
    ef, es = ema(closes, fast), ema(closes, slow)
    line = [a - b if a is not None and b is not None else None for a, b in zip(ef, es)]
    start = next((i for i, v in enumerate(line) if v is not None), None)
    signal = [None] * len(closes)
    if start is not None:
        signal[start:] = ema(line[start:], sig)
    hist = [l - s if l is not None and s is not None else None for l, s in zip(line, signal)]
    return line, signal, hist


def rsi(closes, n=14):
    out = [None] * len(closes)
    if len(closes) <= n:
        return out
    g = l = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        g += max(d, 0)
        l += max(-d, 0)
    ag, al = g / n, l / n
    out[n] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n
        al = (al * (n - 1) + max(-d, 0)) / n
        out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return out


def pivots(series, kind, left=3, right=3):
    idx = []
    for i in range(left, len(series) - right):
        w = series[i - left:i + right + 1]
        if any(x is None for x in w):
            continue
        v = series[i]
        if (kind == "low" and v == min(w)) or (kind == "high" and v == max(w)):
            idx.append(i)
    return idx


def divergence(lows, highs, ind, lookback=80, recent=20):
    """Regular divergence between price swings and an indicator."""
    n = len(ind)
    start = max(0, n - lookback)
    res = "None"
    pl = [i for i in pivots(lows, "low") if i >= start]
    if len(pl) >= 2:
        a, b = pl[-2], pl[-1]
        if (n - 1 - b <= recent and ind[a] is not None and ind[b] is not None
                and lows[b] < lows[a] and ind[b] > ind[a]):
            res = "Bullish"
    ph = [i for i in pivots(highs, "high") if i >= start]
    if len(ph) >= 2:
        a, b = ph[-2], ph[-1]
        if (n - 1 - b <= recent and ind[a] is not None and ind[b] is not None
                and highs[b] > highs[a] and ind[b] < ind[a]):
            res = "Bearish" if res == "None" else "Both"
    return res


# ------------------------------------------------------------------
# Single timeframe analysis
# ------------------------------------------------------------------
def _sgn(x):
    return 1 if x > 0 else -1 if x < 0 else 0


def analyze_tf(instrument_key, tf, headers):
    out = {"tf": tf, "label": TF_LABEL.get(tf, tf)}
    candles = fetch_candles(instrument_key, tf, headers)
    if len(candles) < 55:
        out["error"] = f"Only {len(candles)} candles returned (need 55+)"
        return out

    closes = [float(c[4]) for c in candles]
    highs = [float(c[2]) for c in candles]
    lows = [float(c[3]) for c in candles]
    px = closes[-1]

    # ---- EMA 20 / 50
    e20, e50 = ema(closes, 20), ema(closes, 50)
    state = "PCO" if e20[-1] > e50[-1] else "NCO"
    bars = 0
    for i in range(len(closes) - 1, -1, -1):
        if e20[i] is None or e50[i] is None or (e20[i] > e50[i]) != (state == "PCO"):
            break
        bars += 1
    if px > e20[-1] and px > e50[-1]:
        pstat, ptone = "Above 20 & 50", 1
    elif px < e20[-1] and px < e50[-1]:
        pstat, ptone = "Below 20 & 50", -1
    elif px > e50[-1]:
        pstat, ptone = "Above 50, below 20", 0
    else:
        pstat, ptone = "Above 20, below 50", 0

    # ---- MACD
    ml, ms, mh = macd(closes)
    bias_up = ml[-1] > ms[-1]
    above0 = ml[-1] > 0
    tick_up = ml[-1] > ml[-2]
    hist_up = mh[-1] > mh[-2]
    if bias_up and above0:
        mbias = "Strong Bullish"
    elif bias_up:
        mbias = "Bullish (below zero)"
    elif not above0:
        mbias = "Strong Bearish"
    else:
        mbias = "Bearish (above zero)"

    # ---- RSI
    rs = rsi(closes)
    rv = rs[-1]
    rtick_up = rv > rs[-2]
    zone = ("Overbought" if rv >= 70 else "Bullish" if rv >= 60 else "Neutral" if rv >= 40
            else "Bearish" if rv > 30 else "Oversold")

    # ---- Divergence
    d_rsi = divergence(lows, highs, rs)
    d_macd = divergence(lows, highs, mh)
    dtxt = "None"
    if "Bullish" in (d_rsi, d_macd) and "Bearish" not in (d_rsi, d_macd):
        dtxt = "Bullish"
    elif "Bearish" in (d_rsi, d_macd) and "Bullish" not in (d_rsi, d_macd):
        dtxt = "Bearish"
    elif d_rsi != "None" or d_macd != "None":
        dtxt = "Mixed"
    dtone = {"Bullish": 1, "Bearish": -1}.get(dtxt, 0)

    # ---- Scoring (6 factors, -100..+100)
    pts = [ptone, 1 if state == "PCO" else -1, 1 if bias_up else -1,
           1 if tick_up else -1, _sgn(rv - 50), dtone]
    score = round(sum(pts) / 6 * 100)

    # ---- Narrative
    notes = []
    if bars <= 3:
        notes.append(f"Fresh EMA20/50 {state} - formed {bars} bar(s) ago.")
    if bias_up != tick_up:
        notes.append("MACD bias and tick disagree - momentum may be turning.")
    if rv >= 70:
        notes.append("RSI overbought - chasing risk.")
    elif rv <= 30:
        notes.append("RSI oversold - bounce risk for shorts.")
    if dtxt in ("Bullish", "Bearish"):
        notes.append(f"{dtxt} divergence on spot (RSI: {d_rsi}, MACD hist: {d_macd}).")
    if ptone == 0:
        notes.append("Price is trapped between EMA20 and EMA50 - no clean trend.")

    rows = [
        {"k": "Price vs EMA", "v": pstat, "t": ptone,
         "sub": f"Close {px:.2f} | EMA20 {e20[-1]:.2f} | EMA50 {e50[-1]:.2f}"},
        {"k": "EMA 20/50", "v": f"{state}", "t": 1 if state == "PCO" else -1,
         "sub": f"{'Positive' if state == 'PCO' else 'Negative'} crossover state, {bars} bars"},
        {"k": "MACD bias", "v": mbias, "t": 1 if bias_up else -1,
         "sub": f"Line {ml[-1]:.2f} | Signal {ms[-1]:.2f} | Hist {mh[-1]:.2f}"},
        {"k": "MACD tick", "v": "Up tick" if tick_up else "Down tick", "t": 1 if tick_up else -1,
         "sub": f"Line {ml[-1]:.2f} vs prev {ml[-2]:.2f} | Hist {'rising' if hist_up else 'falling'}"},
        {"k": "RSI 14", "v": f"{rv:.1f} {zone}", "t": _sgn(rv - 50),
         "sub": f"{'Rising' if rtick_up else 'Falling'} (prev {rs[-2]:.1f})"},
        {"k": "Divergence", "v": dtxt, "t": dtone,
         "sub": f"RSI: {d_rsi} | MACD: {d_macd}"},
    ]

    out.update({
        "bars": len(candles), "close": px, "last_candle": candles[-1][0][:16].replace("T", " "),
        "ema20": round(e20[-1], 2), "ema50": round(e50[-1], 2), "cross_state": state,
        "bars_since_cross": bars, "macd_line": round(ml[-1], 3), "macd_signal": round(ms[-1], 3),
        "macd_hist": round(mh[-1], 3), "macd_tick": "UP" if tick_up else "DOWN",
        "macd_bias": mbias, "rsi": round(rv, 1), "rsi_zone": zone,
        "price_status": pstat, "divergence": dtxt,
        "score": score, "rows": rows, "notes": notes,
    })
    return out


# ------------------------------------------------------------------
# Full MTF run + storage
# ------------------------------------------------------------------
_STORE = OrderedDict()
_LOCK = threading.Lock()
_MAX = 300


def run_mtf_analysis(spot_key, signal_tf, direction, headers, meta=None):
    """direction: 'BULL' (CE) or 'BEAR' (PE)."""
    signal_tf = normalize_tf(signal_tf)
    tfs = [signal_tf] + [t for t in HTF_MAP.get(signal_tf, ()) if t != signal_tf]
    with ThreadPoolExecutor(max_workers=len(tfs)) as ex:
        results = list(ex.map(lambda t: analyze_tf(spot_key, t, headers), tfs))
    for r, role in zip(results, ["Signal TF", "HTF 1", "HTF 2"]):
        r["role"] = role

    d = 1 if direction == "BULL" else -1
    weights = [1.0, 1.5, 2.0]
    good = [(r, w) for r, w in zip(results, weights) if "error" not in r]
    if good:
        align = round(sum(r["score"] * d * w for r, w in good) / sum(w for _, w in good))
    else:
        align = 0
    verdict = ("STRONG ALIGNMENT" if align >= 60 else "MODERATE ALIGNMENT" if align >= 25
               else "MIXED / NEUTRAL" if align > -25 else "AGAINST TRADE")

    data = {
        "signal_tf": signal_tf, "signal_tf_label": TF_LABEL[signal_tf],
        "direction": direction, "alignment": align, "verdict": verdict,
        "timeframes": results,
        "generated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        "spot_key": spot_key, "meta": meta or {},
    }
    return data


def save_analysis(signal_id, data):
    with _LOCK:
        _STORE[signal_id] = data
        while len(_STORE) > _MAX:
            _STORE.popitem(last=False)


def get_analysis(signal_id):
    with _LOCK:
        return _STORE.get(signal_id)


def refresh_analysis(signal_id, headers):
    old = get_analysis(signal_id)
    if not old:
        return None
    new = run_mtf_analysis(old["spot_key"], old["signal_tf"], old["direction"], headers, old["meta"])
    save_analysis(signal_id, new)
    return new


def telegram_block(a, link=None):
    if not a:
        return ""
    arrow = {"UP": "⬆", "DOWN": "⬇"}
    lines = ["", f"📊 <b>MTF Analysis</b> | {a['verdict']} ({a['alignment']:+d}%)"]
    for t in a["timeframes"]:
        if "error" in t:
            lines.append(f"<b>{t['label']}</b>: data unavailable")
            continue
        lines.append(
            f"<b>{t['label']}</b> | {t['cross_state']} | {t['price_status']} | "
            f"MACD {t['macd_bias']} {arrow[t['macd_tick']]} | RSI {t['rsi']:.0f} | Div: {t['divergence']}"
        )
    if link:
        lines.append(f'<a href="{link}">Open full analysis</a>')
    return "\n".join(lines)


# ------------------------------------------------------------------
# Full-window analysis page
# ------------------------------------------------------------------
ANALYSIS_HTML = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Signal analysis</title>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@600;800&family=Rajdhani:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bull:#00e676;--bear:#ff3b5c;--gold:#ffd54a;--cyan:#22e5ff;--purple:#a06bff;--ink:#e7ecff;--mute:#8d97bd;
--panel:rgba(16,21,38,.78);--line:rgba(120,140,255,.18);}
*{box-sizing:border-box}
body{margin:0;font-family:'Rajdhani',sans-serif;color:var(--ink);min-height:100vh;
background:radial-gradient(900px 600px at 5% -10%,rgba(160,107,255,.25),transparent 60%),
radial-gradient(900px 600px at 100% 0,rgba(34,229,255,.18),transparent 55%),#070914;}
.wrap{max-width:1280px;margin:0 auto;padding:22px 20px 50px}
.head{display:grid;grid-template-columns:1fr auto;gap:24px;align-items:center;background:var(--panel);
border:1px solid var(--line);border-radius:18px;padding:22px 26px}
.sym{font-family:'Orbitron';font-weight:800;font-size:1.7rem;letter-spacing:.5px}
.sub{color:var(--mute);margin-top:4px;font-size:1rem}
.chips{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px}
.chip{padding:5px 12px;border-radius:999px;font-weight:700;font-size:.85rem;border:1px solid var(--line);background:rgba(255,255,255,.04)}
.chip.bull{color:var(--bull);border-color:rgba(0,230,118,.45);background:rgba(0,230,118,.1)}
.chip.bear{color:var(--bear);border-color:rgba(255,59,92,.45);background:rgba(255,59,92,.1)}
.chip.neu{color:var(--gold);border-color:rgba(255,213,74,.45);background:rgba(255,213,74,.08)}
.gauge{display:flex;align-items:center;gap:18px}
.ring{width:150px;height:150px;border-radius:50%;display:grid;place-items:center;
background:conic-gradient(var(--c) calc(var(--p)*1%),rgba(255,255,255,.08) 0)}
.ring-in{width:120px;height:120px;border-radius:50%;background:#0a0d1a;display:flex;flex-direction:column;align-items:center;justify-content:center}
.ring-in b{font-family:'Orbitron';font-size:1.6rem}
.ring-in span{font-size:.72rem;color:var(--mute);letter-spacing:1px}
.verdict{font-family:'Orbitron';font-weight:700;font-size:1.05rem;max-width:190px;line-height:1.35}
h2{font-family:'Orbitron';font-size:1rem;margin:28px 0 12px;font-weight:700}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:6px 6px 8px;overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:640px}
th,td{padding:12px 14px;text-align:left;border-bottom:1px solid rgba(120,140,255,.09)}
th{color:var(--mute);font-weight:600;font-size:.9rem}
tr:last-child td{border-bottom:0}
td.k{color:var(--mute);font-weight:600;white-space:nowrap}
.cell{display:inline-flex;align-items:center;gap:7px;padding:4px 11px;border-radius:8px;font-weight:600;font-size:.95rem}
.cell.bull{background:rgba(0,230,118,.12);color:var(--bull)}
.cell.bear{background:rgba(255,59,92,.12);color:var(--bear)}
.cell.neu{background:rgba(255,213,74,.1);color:var(--gold)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:16px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:18px 18px 14px}
.card-h{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:4px}
.card-h b{font-family:'Orbitron';font-size:1.25rem}
.card-h span{color:var(--mute);font-size:.85rem}
.bar{height:7px;border-radius:5px;background:rgba(255,255,255,.07);position:relative;margin:10px 0 14px}
.bar i{position:absolute;top:-3px;width:3px;height:13px;border-radius:2px;background:#fff;box-shadow:0 0 8px #fff}
.bar::before{content:"";position:absolute;left:50%;top:0;bottom:0;width:1px;background:rgba(255,255,255,.3)}
.bar u{position:absolute;top:0;bottom:0;border-radius:5px;text-decoration:none}
.line{padding:9px 0;border-top:1px solid rgba(120,140,255,.09)}
.line .r{display:flex;justify-content:space-between;gap:10px;align-items:center}
.line .r span:first-child{color:var(--mute);font-weight:600}
.line small{display:block;color:#6f7aa3;margin-top:3px;font-size:.82rem}
.meter{height:8px;border-radius:5px;position:relative;margin:8px 0 2px;
background:linear-gradient(90deg,var(--bull) 0 30%,rgba(255,255,255,.12) 30% 70%,var(--bear) 70% 100%)}
.meter i{position:absolute;top:-4px;width:4px;height:16px;border-radius:2px;background:#fff;box-shadow:0 0 8px #fff}
.notes{margin:12px 0 0;padding:10px 12px;border-radius:10px;background:rgba(160,107,255,.08);color:#cfd5f5;font-size:.92rem}
.notes div+div{margin-top:4px}
.oi{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}
.oi div{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px 14px}
.oi small{color:var(--mute);font-weight:600}
.oi b{display:block;font-family:'Orbitron';font-size:1.1rem;margin-top:3px}
.bull-t{color:var(--bull)}.bear-t{color:var(--bear)}
button{background:rgba(34,229,255,.12);color:#dff6ff;border:1px solid rgba(34,229,255,.4);border-radius:9px;
padding:7px 14px;font:700 .9rem 'Rajdhani';cursor:pointer}
button:focus-visible{outline:2px solid var(--cyan);outline-offset:2px}
.err{padding:60px 10px;text-align:center;color:var(--bear);font-size:1.1rem}
@media(max-width:720px){.head{grid-template-columns:1fr}}
</style></head><body><div class="wrap" id="app"><div class="err" style="color:var(--mute)">Loading analysis…</div></div>
<script>
const sid = decodeURIComponent(location.pathname.split('/').pop());
const tone = t => t>0?'bull':t<0?'bear':'neu';
const mark = t => t>0?'▲':t<0?'▼':'●';
const esc = s => String(s).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
let last = null;

function card(t){
  if(t.error) return `<div class="card"><div class="card-h"><b>${t.label}</b><span>${t.role}</span></div><div class="err" style="padding:24px 0">${esc(t.error)}</div></div>`;
  const sc = t.score, left = sc>=0?50:50+sc/2, w = Math.abs(sc)/2;
  const col = sc>=0?'var(--bull)':'var(--bear)';
  const lines = t.rows.map(r=>`<div class="line"><div class="r"><span>${r.k}</span><span class="cell ${tone(r.t)}">${mark(r.t)} ${esc(r.v)}</span></div><small>${esc(r.sub)}</small>${
    r.k==='RSI 14'?`<div class="meter"><i style="left:calc(${t.rsi}% - 2px)"></i></div>`:''}</div>`).join('');
  const notes = t.notes.length?`<div class="notes">${t.notes.map(n=>`<div>• ${esc(n)}</div>`).join('')}</div>`:'';
  return `<div class="card"><div class="card-h"><b>${t.label}</b><span>${t.role} · last bar ${t.last_candle}</span></div>
  <div class="bar"><u style="left:${left}%;width:${w}%;background:${col}"></u><i style="left:calc(${50+sc/2}% - 1px)"></i></div>
  <div style="color:var(--mute);font-size:.85rem;margin-top:-6px;margin-bottom:6px">Trend score <b style="color:${col}">${sc>0?'+':''}${sc}</b> (bullish + / bearish −)</div>
  ${lines}${notes}</div>`;
}

function render(a){
  last = a;
  const m = a.meta||{}, ok = a.timeframes.filter(t=>!t.error);
  const p = (a.alignment+100)/2;
  const c = a.alignment>=25?'var(--bull)':a.alignment<=-25?'var(--bear)':'var(--gold)';
  const dirCls = a.direction==='BULL'?'bull':'bear';
  const head = `<div class="head"><div>
    <div class="sym">${esc(m.option_symbol||m.underlying||'Signal')}</div>
    <div class="sub">${esc(m.underlying||'')} spot analysis · alert on ${a.signal_tf_label} chart · ${esc(m.alert_time||'')}</div>
    <div class="chips"><span class="chip ${dirCls}">${a.direction==='BULL'?'Bullish signal (CE)':'Bearish signal (PE)'}</span>
    <span class="chip">Spot ${ok.length?ok[0].close:'N/A'}</span>
    <span class="chip">Updated ${a.generated_at}</span>
    <button id="rf">Refresh now</button></div></div>
    <div class="gauge"><div class="verdict" style="color:${c}">${a.verdict}</div>
    <div class="ring" style="--p:${p};--c:${c}"><div class="ring-in"><b>${a.alignment>0?'+':''}${a.alignment}%</b><span>ALIGNMENT</span></div></div></div></div>`;

  const hdr = a.timeframes.map(t=>`<th>${t.label}<br><span style="font-weight:500">${t.role}</span></th>`).join('');
  const keys = ok.length?ok[0].rows.map(r=>r.k):[];
  const body = keys.map((k,i)=>`<tr><td class="k">${k}</td>${a.timeframes.map(t=>t.error?'<td>–</td>':
    `<td><span class="cell ${tone(t.rows[i].t)}">${mark(t.rows[i].t)} ${esc(t.rows[i].v)}</span></td>`).join('')}</tr>`).join('');
  const matrix = `<h2>Confluence matrix</h2><div class="panel"><table><thead><tr><th></th>${hdr}</tr></thead><tbody>${body}</tbody></table></div>`;

  document.getElementById('app').innerHTML = head + matrix +
    `<h2>Timeframe detail</h2><div class="cards">${a.timeframes.map(card).join('')}</div>` +
    `<h2>Open interest & strikes</h2><div class="oi" id="oi"><div><small>Loading…</small></div></div>`;
  document.getElementById('rf').onclick = () => load(true);
}

function pct(v){ if(!v||v==='N/A') return 'N/A'; const n=parseFloat(v); return `<span class="${n<0?'bear-t':'bull-t'}">${v}</span>`; }
async function loadOi(){
  const box = document.getElementById('oi'); if(!box) return;
  try{
    const d = await (await fetch('/api/details/'+encodeURIComponent(sid))).json();
    if(d.status!=='ok'){ box.innerHTML='<div><small>OI snapshot</small><b>Not available for this signal</b></div>'; return; }
    const cell=(l,v)=>`<div><small>${l}</small><b>${v}</b></div>`;
    box.innerHTML = cell('Option LTP',d.option_ltp)+cell('OI decay (signal strike)',pct(d.option_oi_change_pct))+
      cell(`OTM1 ${d.otm1_strike} decay`,pct(d.otm1_decay))+cell(`OTM2 ${d.otm2_strike} decay`,pct(d.otm2_decay))+
      cell(`Opposite ${d.opposite_type} ${d.opposite_strike} decay`,pct(d.opposite_decay))+
      cell('Lot size',d.lot_size)+cell('Estimated cost','₹ '+d.estimated_cost);
  }catch(e){ box.innerHTML='<div><small>OI snapshot</small><b>Failed to load</b></div>'; }
}
async function load(refresh){
  try{
    const r = await fetch('/api/analysis/'+encodeURIComponent(sid)+(refresh?'?refresh=1':''));
    if(!r.ok){ document.getElementById('app').innerHTML='<div class="err">No analysis stored for this signal. It may have expired after a server restart.</div>'; return; }
    render(await r.json()); loadOi();
  }catch(e){ if(!last) document.getElementById('app').innerHTML='<div class="err">Could not reach the server.</div>'; }
}
load(false);
setInterval(()=>load(true), 60000);
</script></body></html>
"""
