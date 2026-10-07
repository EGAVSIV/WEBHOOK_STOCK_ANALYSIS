# Integration + deploy guide

## 1. Repo layout (GitHub)

```
app.py                  <- your existing script (with the patches below)
analysis.py             <- new
requirements.txt        <- new
render.yaml             <- new
.gitignore              <- add:  .env  __pycache__/
```
Do NOT commit your Upstox token or Telegram token. Set them in Render.

`requirements.txt`
```
flask
gunicorn
pytz
requests
```

`render.yaml`
```yaml
services:
  - type: web
    name: oi-gate-webhook
    runtime: python
    plan: starter            # free plan sleeps -> TradingView alerts get missed
    buildCommand: >
      pip install -r requirements.txt &&
      curl -L -o NSE.json.gz https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz
    startCommand: gunicorn app:app --workers 1 --threads 8 --timeout 60
    envVars:
      - key: UPSTOX_ACCESS_TOKEN
        sync: false
      - key: TELEGRAM_BOT_TOKEN
        sync: false
      - key: TELEGRAM_CHAT_ID
        sync: false
      - key: WEBHOOK_SECRET
        sync: false
      - key: PUBLIC_BASE_URL     # e.g. https://oi-gate-webhook.onrender.com
        sync: false
      - key: TEST_MODE
        value: "true"
      - key: OI_DROP_THRESHOLD
        value: "-5.0"
```
`--workers 1` is required: stats, logs and analysis are kept in memory.

## 2. Patches to app.py

**(a) Imports + config** (top of file, after the other imports/config)
```python
import analysis
from flask import Response

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "").strip()
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")
```

**(b) Secret check** - first lines inside `def webhook():` after `global trade_counter`
```python
    if WEBHOOK_SECRET and request.args.get("key") != WEBHOOK_SECRET:
        return jsonify({"status": "error", "reason": "unauthorized"}), 401
```

**(c) Run the analysis** - in `webhook()`, directly ABOVE the comment block
`# OI DECAY ANALYSIS`
```python
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
```

**(d) Flag it in the log row** - add one line inside the `log_detail = {...}` dict
```python
        "has_analysis": bool(mtf),
```

**(e) Add to Telegram** - right after `tg_text = format_telegram_message(...)` and before `notify_telegram(tg_text)`
```python
    link = f"{PUBLIC_BASE_URL}/analysis/{signal_id}" if PUBLIC_BASE_URL else None
    tg_text += "\n" + analysis.telegram_block(mtf, link)
```

**(f) New routes** - paste above the `# DASHBOARD` section
```python
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
```

**(g) Dashboard button** - inside `DASHBOARD_HTML`

1. Add a header cell after `<th>Details</th>`:
```html
<th>Analysis</th>
```
2. In `renderTable`, change both `colspan="12"` to `colspan="13"` and add after the "View OI" `<td>`:
```js
      <td><button class="details-btn" ${(r.detail && r.detail.has_analysis) ? '' : 'disabled'}
          onclick="window.open('/analysis/${sid}','an_${sid}','width=1280,height=900')">📊 Analysis</button></td>
```

## 3. TradingView setup

1. Pine Editor -> paste `ema_5_50_webhook.pine` -> Add to chart (put it on the chart of the symbol, spot OR option strike).
2. Create alert -> Condition: this indicator -> **Any alert() function call**.
3. Notifications -> Webhook URL: `https://<your-app>.onrender.com/webhook?key=<WEBHOOK_SECRET>`
4. Leave the message box as-is (the script supplies the text). Webhooks need a paid TradingView plan.
5. One alert per symbol/timeframe you want to watch.

HTF mapping used: 5m -> 15m + 1H, 15m -> 1H + 4H, 1H -> 4H + 1D, 4H -> 1D + 1W, 1D -> 1W + 1M.

## 4. Things to know

- **Upstox token expires daily.** Update `UPSTOX_ACCESS_TOKEN` in Render each morning, or automate the login flow.
- **TradingView drops a webhook if the server takes longer than ~3 seconds.** Your existing handler already makes many sequential API calls; the analysis fetches the 3 timeframes in parallel to limit the added delay. If you see missed signals, move the body of `webhook()` into a background thread and return `200` immediately.
- **Candle API:** `analysis.fetch_candles()` uses Upstox's v3 historical + intraday candle endpoints (custom intervals like 15m/1H/4H). If Upstox rejects a path or unit, that is the only function to change; the Analysis window will show "Only 0 candles returned" for that timeframe.
- Option signals: the alert is on the option chart, but trend analysis is done on the **underlying spot**. Direction = CE -> bullish, PE -> bearish.
- "PCO / NCO" is read as EMA20 above / below EMA50 (positive / negative crossover state).
