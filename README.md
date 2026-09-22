# Memecoin Scanner v3.3.6 Pro On-Chain

Changes:
- Solana Early-Buyer RPC now has automatic fallback support.
- Default fallback: `https://solana-rpc.publicnode.com`.
- `SOLANA_RPC_FALLBACKS` can contain comma-separated additional RPC URLs.
- `/scan` now distinguishes RPC/no-data failures from real zero-buyer results.
- Shows sampled Solana signatures and parsed transactions when buyer data is missing.
- A coin is only a TOP-EARLY candidate when at least 2 buyer wallets were actually detected on-chain.
- Market score can no longer by itself create a TOP-EARLY alert.

No private key or real-money auto-trading is included.

## v3.3.7 Pro Direct Solana
- Solana discovery now queries configured launch/AMM program IDs directly via RPC (`getSignaturesForAddress`).
- WebSocket program-log signatures are cached briefly and fed into discovery.
- Parsed transactions are inspected for SPL token mints before optional market-data enrichment.
- `/scan` source diagnostics include `solana_direct=sigs:... mints:... pairs:... enrich429:...`.
- DexScreener/GeckoTerminal remain best-effort enrichers/fallbacks; their HTTP 429 responses no longer prevent direct on-chain discovery from running.
- New optional env vars: `SOLANA_DIRECT_LIMIT` (default 25), `SOLANA_DIRECT_TTL_SECONDS` (default 900).
