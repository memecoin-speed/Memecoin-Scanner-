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
