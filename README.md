# Memecoin Scanner v3.7.0

Telegram bot for early Solana and Ethereum pool discovery, on-chain buyer checks, alerts and paper-entry logging. It **never executes real trades** and does not need a wallet or private key.

## Set up

1. Create a Telegram bot with BotFather and copy the token.
2. Set `TELEGRAM_BOT_TOKEN` and your numeric `ALLOWED_CHAT_ID` as host environment variables. The bot refuses to start without either setting. Do not commit a real token or a populated `.env` file.
3. Deploy this repository as a web service with `Dockerfile` on Render, or run `pip install -r requirements.txt && python app.py`. The service listens on `$PORT` at `/health`. Use exactly one running instance per bot token (Telegram long polling).
4. In Telegram, send `/start`, `/status` and `/scan`. Alerts and `/paper` are available in the allowed chat. An alert's “Paper Buy” button saves an entry price and amount; it is **not** a live P&L tracker.

The default public RPC endpoints can rate limit heavily. For sustained operation, provide your own `SOLANA_RPC`, comma-separated `SOLANA_RPC_FALLBACKS`, and optionally `ETH_RPC_URL` and `SOLANA_WS`. A persistent volume for `DB_FILE` is needed if alerts, cached pools and paper entries must survive redeployment. A sleeping or suspended host cannot provide continuous alerts.

## How a candidate qualifies

- Fresh market data must pass liquidity, volume, activity, age and score filters. A pair present only in the persistent cache can enter Solana diagnostics but cannot trigger an alert.
- Solana: the parsed transaction must show target-token inflow to a signing wallet and SOL or stablecoin spend in that transaction. Two distinct, non-dust buyers are required.
- Ethereum: currently supports direct buyer recipients in WETH, USDC, USDT and DAI pools. Receipt logs must show the pool sending the token to the transaction sender and quote-token payment into the pool. Only the earliest bounded transaction sample of pools around 11 hours old or younger is inspected. Router custody, other quote assets, incomplete RPC responses and older pools do not pass the buyer gate.
- These are conservative heuristics, **not** proof of profitability or protection from rugs. Review the token, liquidity and contract separately before acting.

## Changes since v3.6.3

- Solana direct discovery rotates through fresh signatures instead of repeating the first batch; WS triggers are rate limited.
- Ethereum now checks payment receipts instead of labeling transfer recipients as verified buyers.
- Incomplete analysis and cached-only market data cannot produce alerts.
- Long Telegram diagnostics are sent in safe-sized chunks. Configuration and old `/buy` text were corrected.

Run offline checks with `python -m unittest discover -s tests -v`.
