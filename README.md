# Memecoin Scanner v3.4.4 Pro Fast Discovery

- Fast/fault-isolated discovery: DexScreener, GeckoTerminal and Solana Direct run independently.
- Short per-source watchdogs: a slow provider is skipped instead of blocking the scan.
- Faster RPC and candidate budgets while Telegram /start and /status remain responsive.
- Strict TOP-EARLY gate remains: at least 2 distinct verified buyer wallets are required.
- Paper trading only; no private keys or automatic real-money execution.

Version: `3.4.4-pro-fast-discovery`


## v3.4.6
Discovery pipeline fix: cache is loaded before network calls, providers are isolated, profile enrichment runs concurrently, and diagnostics always show pipeline/provider state.
