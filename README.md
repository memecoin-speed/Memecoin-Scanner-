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

## v3.4.1 stability
- `/scan` runs in a background task so `/start` and `/status` stay responsive.
- Manual and automatic scans are serialized to avoid RPC request storms.
- Telegram HTTP timeouts are increased and scan execution has a 4-minute ceiling.
- The strict >=2 verified buyer-wallet gate remains enabled.

## v3.4.2 resilience
- DexScreener retry/backoff plus 15-minute successful-response cache.
- Token-pair enrichment cache so temporary 429 responses do not instantly erase direct Solana discovery.
- Solana RPC concurrency limit and retry/backoff with stage-specific error counters.
- Final TOP-EARLY output remains impossible without at least 2 concrete unique buyer wallets.

## v3.4.3 Pro Watchdog
- harte Timeouts pro Solana-RPC-Aufruf (Standard 10s)
- Discovery-Watchdog (Standard 70s)
- Kandidaten-Watchdog (Standard 55s)
- begrenzte Solana-Transaktionsanalyse pro Kandidat
- Fortschritts-Checkpoints im manuellen Telegram-Scan
- zusätzliche Watchdog-Diagnose im Scan-Ergebnis
- striktes Early-Buyer-Gate (mindestens 2 konkrete unterschiedliche Wallets) bleibt aktiv

Optionale Environment-Variablen: `RPC_CALL_TIMEOUT`, `CANDIDATE_ANALYSIS_TIMEOUT`, `DISCOVERY_STAGE_TIMEOUT`, `SOLANA_SIGNATURE_LIMIT`.
