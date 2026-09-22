# Memecoin Scanner v3.3.4 Pro Multisource

Fix für `Profile geladen: 0`: Die Discovery hängt nicht mehr an nur einem DexScreener-Feed.

## Neu
- kombiniert `token-profiles/latest`, `token-boosts/latest` und `token-boosts/top`
- dedupliziert Solana-/Ethereum-Tokens vor der Pair-Abfrage
- zeigt den Status jeder Discovery-Quelle direkt in `/scan`
- eine ausgefallene/leer zurückkommende Quelle setzt den gesamten Scanner nicht mehr auf 0
- Versionskennung: `3.3.4-pro-multisource`

Die bestehende Early-Buyer-, Risiko-, Telegram- und Paper-Trading-Logik bleibt erhalten.
