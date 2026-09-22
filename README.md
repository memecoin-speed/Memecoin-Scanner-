# Memecoin Scanner v3.4.0 Pro Strict Buyer Gate

- Final candidate list is rebuilt only from tokens with at least 2 concrete, unique on-chain buyer wallet addresses.
- A reported buyer_count alone is not sufficient.
- The same strict postcondition protects Telegram output and the background alert cache.
- Discovery and existing buyer diagnostics are retained.

# Memecoin Scanner v3.3.9 Pro Buyer Gate

This build adds defense-in-depth for Early Buyer alerts:

- A token can only be displayed or auto-alerted as TOP-EARLY when at least 2 on-chain buyer wallets were verified.
- The gate exists both in analysis and again immediately before Telegram output/auto-alerts.
- `/scan` now prints Buyer-Diagnose: signatures checked, transactions parsed, RPC errors, and candidates rejected for missing buyer evidence.
- Discovery behavior from v3.3.8 remains unchanged.

Use `/start` after deployment and verify `Version: 3.3.9-pro-buyer-gate`, then run `/scan`.
