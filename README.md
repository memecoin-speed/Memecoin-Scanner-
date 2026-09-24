# Memecoin Scanner v3.4.9 Pro Direct Solana Pools

Direct Solana discovery now resolves discovered mints through Raydium API v3 `/pools/info/mint` before falling back to DexScreener. This is intended to turn on-chain `mints` into usable `pairs` even when DexScreener/Gecko are rate limited.

Safety: paper trading only; no private keys or automatic real-money execution.

# Memecoin Scanner v3.4.4 Pro Fast Discovery

- Fast/fault-isolated discovery: DexScreener, GeckoTerminal and Solana Direct run independently.
- Short per-source watchdogs: a slow provider is skipped instead of blocking the scan.
- Faster RPC and candidate budgets while Telegram /start and /status remain responsive.
- Strict TOP-EARLY gate remains: at least 2 distinct verified buyer wallets are required.
- Paper trading only; no private keys or automatic real-money execution.

Version: `3.4.4-pro-fast-discovery`


## v3.4.6
Discovery pipeline fix: cache is loaded before network calls, providers are isolated, profile enrichment runs concurrently, and diagnostics always show pipeline/provider state.

## v3.4.7 Pro Solana Fast Path
- Solana direct discovery now uses a bounded incremental fast path.
- Only a small recent signature/transaction batch is processed per scan.
- RPC calls are parallelized within the existing semaphore limits.
- Solana mint enrichment is bounded per token so one slow provider cannot stall discovery.
- Successful pairs continue to be persisted in SQLite for later scans.

## v3.4.8 Buyer Precheck
- Up to 3 young Solana pairs can enter a relaxed diagnostic buyer precheck.
- The normal market filters remain unchanged for real alerts.
- Precheck-only pairs can never appear as TOP-EARLY or trigger an alert.
- Goal: exercise Solana signature/transaction parsing even when the strict market filter has zero candidates.

## v3.5.4
- Per-coin buyer diagnostics: signatures, parsed/attempted TXs, wallet candidates, verified swap buyers, market/precheck status, and buyer-gate result.
- Keeps the verified-buyer gate and discovery behavior from v3.5.2 unchanged.


## v3.5.4 Ultra-Early Precheck
- Solana pools younger than MIN_PAIR_AGE_MINUTES may enter the diagnostic buyer precheck even before liquidity/volume mature.
- Ultra-early precheck remains diagnostic-only and can never generate EARLY ALERT/TOP-EARLY unless the strict market gate passes.
- Maximum diagnostic precheck remains capped at 3, prioritizing the youngest ultra-early pools.


## v3.5.5 Ultra-Early Selection Fix
- Fixes ultra-new Solana selection when provider/cache 24h transaction counters are still zero or missing.
- Any otherwise valid Solana pair younger than `MIN_PAIR_AGE_MINUTES` can enter the capped diagnostic precheck.
- Ultra-early candidates remain diagnostic-only; strict market pass + verified buyer gate are still required for alerts.
- Precheck remains capped at 3 and prioritizes ultra-early pairs first.


## v3.5.9 Strong Buyer Gate
Alerts require at least 2 verified swap buyers and at least 2 non-Dust (meaningful) buyers. Dust activity remains visible in diagnostics but cannot satisfy the quality gate. Precheck-only pairs remain non-alerting.


## v3.6.0 Buyer Quality Accounting Fix
Strong, Normal and Dust are now mutually exclusive buyer buckets. Qualified buyers are exactly Strong + Normal, and diagnostics show Qualified X/2 explicitly. Alert eligibility still requires at least two verified swap buyers and at least two qualified buyers.


## v3.6.3
- Multi-RPC fallback: one attempt per endpoint, then rotate on HTTP/RPC/timeout errors.
- Buyer diagnostics distinguish a true zero-buyer result from an RPC-incomplete analysis.

## v3.6.2
- Bounded one-time retry for transient Solana getTransaction RPC/HTTP errors and local stage timeouts.
- Buyer quality diagnostics now display qualified buyer count with the minimum explicitly.
- Market filters, buyer-quality thresholds, and alert gates are unchanged.
