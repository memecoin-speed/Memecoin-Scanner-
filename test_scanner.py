import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import app


class ScannerTests(unittest.IsolatedAsyncioTestCase):
    async def test_persistent_cache_cannot_create_alert_candidate(self):
        class Session:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *_args):
                pass

        pair = {"chainId": "solana", "pairAddress": "pool", "baseToken":
                {"address": "mint", "name": "Pepe", "symbol": "PEPE"},
                "liquidity": {"usd": 15000}, "volume": {"h24": 20000},
                "txns": {"h24": {"buys": 100, "sells": 90}},
                "priceChange": {"h24": 20},
                "pairCreatedAt": int((app.time.time() - 3600) * 1000)}
        with patch.object(app, "load_persistent_pairs", return_value=[pair]), \
             patch.object(app.aiohttp, "ClientSession", return_value=Session()), \
             patch.object(app, "get_gecko_new_pools", AsyncMock(return_value=([], "ok"))), \
             patch.object(app, "get_discovery_profiles", AsyncMock(return_value=([], {}))), \
             patch.object(app, "get_direct_solana_pairs", AsyncMock(return_value=([], {}))):
            candidates, diagnostics = await app.discover_candidates()
        self.assertEqual(diagnostics["cache_only"], 1)
        self.assertFalse(any(c["strict_market_pass"] for c in candidates))

    async def test_direct_solana_rotates_signatures(self):
        app.solana_direct_signatures.clear()
        app.solana_direct_attempted.clear()
        signatures = [{"signature": f"sig-{i}", "blockTime": 1000 - i}
                      for i in range(8)]

        async def rpc(_session, method, params):
            if method == "getSignaturesForAddress":
                return signatures, "rpc", []
            return {"signature": params[0]}, "rpc", []

        async def enrich(_session, mint):
            return [{"pairAddress": mint}], "ok"

        with patch.object(app, "SOLANA_PROGRAM_IDS", ["program"]), \
             patch.object(app, "SOLANA_DIRECT_TX_LIMIT", 3), \
             patch.object(app, "solana_rpc_with_fallback", side_effect=rpc), \
             patch.object(app, "_extract_mints_from_parsed_tx",
                          side_effect=lambda tx: [tx["signature"]]), \
             patch.object(app, "get_raydium_pairs_for_mint", side_effect=enrich), \
             patch.object(app.time, "time", return_value=1000):
            first, first_diag = await app.get_direct_solana_pairs(None)
            second, second_diag = await app.get_direct_solana_pairs(None)
        self.assertEqual(first_diag["signatures"], 3)
        self.assertEqual(second_diag["signatures"], 3)
        self.assertTrue({p["pairAddress"] for p in first}.isdisjoint(
            {p["pairAddress"] for p in second}))

    async def test_ethereum_requires_quote_payment_in_receipt(self):
        pair = "0x" + "1" * 40
        token = "0x" + "2" * 40
        quote = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        wallets = ["0x" + "3" * 40, "0x" + "4" * 40]
        topic = app.ERC20_TRANSFER_TOPIC
        def transfer(address, sender, recipient, amount):
            return {"address": address, "topics": [topic, app.topic_address(sender),
                    app.topic_address(recipient)], "data": hex(amount)}
        logs = [{**transfer(token, pair, wallet, 100), "transactionHash": f"0xhash{i}",
                 "blockNumber": hex(100 + i)} for i, wallet in enumerate(wallets)]
        include_payment = True

        async def rpc(_session, _url, method, params):
            i = int(params[0][-1])
            if method == "eth_getTransactionByHash":
                return {"from": wallets[i]}
            receipt_logs = [logs[i]]
            if include_payment:
                receipt_logs.append(transfer(quote, wallets[i], pair, 10**16))
            return {"status": "0x1", "logs": receipt_logs}

        candidate = {"pair_address": pair, "address": token,
                     "quote_address": quote, "age_hours": 1}
        with patch.object(app, "eth_get_block_number", AsyncMock(return_value=200)), \
             patch.object(app, "eth_get_logs", AsyncMock(side_effect=[logs])), \
             patch.object(app, "eth_get_code", AsyncMock(return_value="0x")), \
             patch.object(app, "rpc_call", side_effect=rpc):
            result = await app.ethereum_early_buyers(None, candidate)
        self.assertEqual(result["buyer_count"], 2)
        self.assertEqual(result["qualified_buyers"], 2)
        self.assertTrue(app.buyer_gate_status({"early_buyers": result})["pass"])

        include_payment = False
        with patch.object(app, "eth_get_block_number", AsyncMock(return_value=200)), \
             patch.object(app, "eth_get_logs", AsyncMock(side_effect=[logs])), \
             patch.object(app, "eth_get_code", AsyncMock(return_value="0x")), \
             patch.object(app, "rpc_call", side_effect=rpc):
            no_payment = await app.ethereum_early_buyers(None, candidate)
        self.assertEqual(no_payment["buyer_count"], 0)

    def test_incomplete_analysis_and_telegram_chunks(self):
        buyers = [{"wallet": "wallet-a"}, {"wallet": "wallet-b"}]
        candidate = {"early_buyers": {"buyers": buyers, "buyer_count": 2,
                     "qualified_buyers": 2, "analysis_complete": False}}
        self.assertFalse(app.buyer_gate_status(candidate)["pass"])
        chunks = app.split_telegram_text("Header\n" + "detail line\n" * 800)
        self.assertGreater(len(chunks), 1)
        self.assertEqual("".join(chunks), "Header\n" + "detail line\n" * 800)
        self.assertTrue(all(len(chunk) <= 3900 for chunk in chunks))


if __name__ == "__main__":
    unittest.main()
