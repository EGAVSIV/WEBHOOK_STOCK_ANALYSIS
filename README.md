# OI Gate Webhook + Multi-Timeframe Analysis

TradingView EMA 5x50 alert -> Render webhook -> Upstox OI decay + MTF spot analysis -> Telegram + dashboard.

## Files
| File | Purpose |
|---|---|
| app.py | Flask app (webhook, dashboard, APIs) |
| analysis.py | EMA20/50, MACD, RSI, divergence on signal TF + 2 higher TFs, full analysis page |
| ema_5_50_webhook.pine | TradingView script (paste in Pine Editor) |
| requirements.txt | Python deps |
| NSE.json.gz | NOT included - download (see below) and add to repo root |

## Instrument file
Download https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz and upload it to the repo root,
or use the Render Build Command shown below to fetch it on every deploy.

## Render settings (Web Service)
- Build Command: `pip install -r requirements.txt && curl -L -o NSE.json.gz https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz`
- Start Command: `gunicorn app:app --workers 1 --threads 8 --timeout 60`
- Environment variables:
  - UPSTOX_ACCESS_TOKEN (changes daily)
  - TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  - WEBHOOK_SECRET (any long random string)
  - PUBLIC_BASE_URL (https://<your-service>.onrender.com)
  - TEST_MODE=true (set false only for live orders)
  - OI_DROP_THRESHOLD=-5.0 (optional), ASYNC_WEBHOOK=true (optional)
- Python version: this bundle includes `.python-version` (3.12.7). You can also set PYTHON_VERSION in Render.

## TradingView
1. Paste ema_5_50_webhook.pine into Pine Editor, add to chart.
2. Create alert -> Condition: the indicator -> "Any alert() function call".
3. Webhook URL: `https://<your-service>.onrender.com/webhook?key=<WEBHOOK_SECRET>`
4. Leave message box as is.

Check `https://<your-service>.onrender.com/health` after deploy.
