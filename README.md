# Memecoin Scanner 3.3.1 Pro

Telegram-Scanner für sehr junge Memecoin-Pairs mit DexScreener-Discovery, Ethereum-/Solana-On-Chain-Analyse, Early-Buyer-Erkennung, Solana-WebSocket-Triggern, Risiko-Filtern, Auto-Alerts und Paper-Trading.

## Was verbessert wurde

- echter Solana-WebSocket-Watcher als Low-Latency-Trigger (pump.fun, PumpSwap, Raydium CPMM)
- Ethereum-ERC20-Transfer-Topic korrigiert
- Early-Buyer-Erkennung für Ethereum und Solana
- Solana Mint-/Freeze-Authority-Risikocheck
- automatische Telegram-Alerts mit Cooldown/Deduplizierung
- Paper-Buy-Button in Auto-Alerts
- SQLite-Persistenz für Alerts und Paper-Trades
- HTTP Health Endpoint `/health` und `PORT`-Binding für Render Web Services
- Scanner bleibt zusätzlich im festen Intervall aktiv, falls WebSocket/RPC ausfällt

## Telegram-Befehle

- `/start` – Übersicht
- `/status` – Bot-/Filterstatus
- `/scan` – manueller Scan
- `/paper` – letzte Paper-Trades
- `/buy` – Hinweis zum Paper Trading

## Render

Als Web Service deployen. Der Bot bindet automatisch an `PORT` und beantwortet `/health`, wodurch der frühere Fehler „no open ports detected“ vermieden wird.

Build Command: `pip install -r requirements.txt`

Start Command: `python app.py`

Mindestens diese Environment Variables setzen:

- `TELEGRAM_BOT_TOKEN`
- `ALLOWED_CHAT_ID`

Für zuverlässige und schnelle On-Chain-Erkennung ist ein eigener Solana-RPC/WebSocket-Anbieter besser als der öffentliche Mainnet-Endpunkt, da öffentliche RPCs Rate-Limits haben können.

## Sicherheit

Der Bot führt **keine echten Käufe** aus und benötigt **keinen Private Key**. `/buy`/Paper-Buy arbeitet nur mit simulierten Einträgen in SQLite. Memecoins bleiben hochriskant; Score und Risk-Check sind Signale, keine Garantie.
