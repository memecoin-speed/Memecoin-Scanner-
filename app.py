import os
import json
import time
import sqlite3
import asyncio
from datetime import datetime, timezone

import aiohttp
import websockets
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

load_dotenv()


# ============================================================
# CONFIG
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ALLOWED_CHAT_ID = os.getenv("ALLOWED_CHAT_ID", "").strip()

DEXSCREENER_API = "https://api.dexscreener.com"

SOLANA_RPC = os.getenv(
    "SOLANA_RPC",
    "https://api.mainnet-beta.solana.com"
)

SOLANA_WS = os.getenv(
    "SOLANA_WS",
    "wss://api.mainnet-beta.solana.com"
)

SCAN_INTERVAL = int(
    os.getenv("SCAN_INTERVAL", "30")
)

MIN_LIQUIDITY_USD = float(
    os.getenv("MIN_LIQUIDITY_USD", "5000")
)

MIN_VOLUME_24H = float(
    os.getenv("MIN_VOLUME_24H", "5000")
)

ALERT_SCORE = int(
    os.getenv("ALERT_SCORE", "65")
)

DB_FILE = os.getenv(
    "DB_FILE",
    "scanner.db"
)

PUMP_FUN_PROGRAM = (
    "6EF8rrecthR5Dkzon8Nwu78kV3Zqj3J5X1V9YvY8F"
)


# ============================================================
# GLOBALS
# ============================================================

http_session = None
last_alerts = {}

scan_task = None
ws_task = None


# ============================================================
# DATABASE
# ============================================================

def db_connect():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db_connect()

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain TEXT NOT NULL,
            token_address TEXT NOT NULL,
            symbol TEXT,
            entry_price REAL NOT NULL,
            amount_usd REAL NOT NULL,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            exit_price REAL,
            pnl_usd REAL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain TEXT,
            token_address TEXT,
            symbol TEXT,
            score INTEGER,
            liquidity REAL,
            volume REAL,
            created_at TEXT
        )
        """
    )

    conn.commit()
    conn.close()


# ============================================================
# HELPERS
# ============================================================

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def allowed(update: Update):
    if not ALLOWED_CHAT_ID:
        return True

    chat = update.effective_chat

    if not chat:
        return False

    return str(chat.id) == ALLOWED_CHAT_ID


def fmt_money(value):
    if value is None:
        return "n/a"

    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"

    if value >= 1_000:
        return f"${value / 1_000:.1f}K"

    return f"${value:.2f}"


def fmt_price(value):
    if value is None:
        return "n/a"

    if value == 0:
        return "0"

    if value < 0.000001:
        return f"{value:.10f}"

    if value < 0.01:
        return f"{value:.8f}"

    return f"{value:.6f}"


def token_key(pair):
    return (
        f"{pair.get('chainId')}:"
        f"{pair.get('baseToken', {}).get('address')}"
    )


# ============================================================
# HTTP
# ============================================================

async def get_http_session():
    global http_session

    if http_session is None:
        timeout = aiohttp.ClientTimeout(total=20)

        http_session = aiohttp.ClientSession(
            timeout=timeout
        )

    return http_session


async def get_json(url, params=None):
    session = await get_http_session()

    try:
        async with session.get(
            url,
            params=params,
            headers={
                "Accept": "application/json"
            }
        ) as response:

            if response.status != 200:
                print(
                    f"HTTP {response.status}: {url}"
                )
                return None

            return await response.json()

    except Exception as exc:
        print(f"HTTP error: {exc}")
        return None


# ============================================================
# DEXSCREENER
# ============================================================

async def get_token_pairs(chain, address):
    data = await get_json(
        f"{DEXSCREENER_API}/latest/dex/tokens/{address}"
    )

    if not data:
        return []

    pairs = data.get("pairs") or []

    return [
        pair
        for pair in pairs
        if pair.get("chainId") == chain
    ]


async def get_latest_token_profiles():
    data = await get_json(
        f"{DEXSCREENER_API}/token-profiles/latest/v1"
    )

    if not isinstance(data, list):
        return []

    return data


# ============================================================
# ANALYSIS
# ============================================================

def calculate_score(pair):
    liquidity = float(
        (pair.get("liquidity") or {}).get("usd") or 0
    )

    volume = float(
        (pair.get("volume") or {}).get("h24") or 0
    )

    txns = pair.get("txns") or {}
    h24 = txns.get("h24") or {}

    buys = int(h24.get("buys") or 0)
    sells = int(h24.get("sells") or 0)

    price_change = float(
        (pair.get("priceChange") or {}).get("h24") or 0
    )

    score = 0

    # Liquidität
    if liquidity >= 100_000:
        score += 25
    elif liquidity >= 50_000:
        score += 20
    elif liquidity >= 20_000:
        score += 15
    elif liquidity >= MIN_LIQUIDITY_USD:
        score += 10

    # Volumen
    if volume >= 500_000:
        score += 25
    elif volume >= 100_000:
        score += 20
    elif volume >= 25_000:
        score += 15
    elif volume >= MIN_VOLUME_24H:
        score += 10

    # Buy pressure
    total_txns = buys + sells

    if total_txns > 0:
        buy_ratio = buys / total_txns

        if buy_ratio >= 0.70:
            score += 20
        elif buy_ratio >= 0.60:
            score += 15
        elif buy_ratio >= 0.50:
            score += 8

    # Momentum
    if 5 <= price_change <= 100:
        score += 10
    elif price_change > 100:
        score += 5

    # Alter des Pairs
    created = pair.get("pairCreatedAt")

    if created:
        age_minutes = max(
            0,
            (time.time() * 1000 - created) / 60_000
        )

        if age_minutes <= 10:
            score += 20
        elif age_minutes <= 30:
            score += 15
        elif age_minutes <= 60:
            score += 10
        elif age_minutes <= 180:
            score += 5

    return min(score, 100)


def analyze_pair(pair):
    liquidity = float(
        (pair.get("liquidity") or {}).get("usd") or 0
    )

    volume = float(
        (pair.get("volume") or {}).get("h24") or 0
    )

    txns = pair.get("txns") or {}
    h24 = txns.get("h24") or {}

    buys = int(h24.get("buys") or 0)
    sells = int(h24.get("sells") or 0)

    price_change = float(
        (pair.get("priceChange") or {}).get("h24") or 0
    )

    score = calculate_score(pair)

    risks = []

    if liquidity < MIN_LIQUIDITY_USD:
        risks.append("Niedrige Liquidität")

    if volume < MIN_VOLUME_24H:
        risks.append("Niedriges Volumen")

    if sells > buys * 1.5 and sells > 20:
        risks.append("Starker Verkaufsdruck")

    if price_change > 200:
        risks.append("Extremer Preisanstieg")

    if liquidity > 0 and volume / liquidity > 50:
        risks.append(
            "Sehr hohes Volumen/Liquidität-Verhältnis"
        )

    return {
        "score": score,
        "liquidity": liquidity,
        "volume": volume,
        "buys": buys,
        "sells": sells,
        "price_change": price_change,
        "risks": risks,
    }


# ============================================================
# SOLANA RPC
# ============================================================

async def solana_rpc(method, params):
    session = await get_http_session()

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }

    try:
        async with session.post(
            SOLANA_RPC,
            json=payload,
            headers={
                "Content-Type": "application/json"
            }
        ) as response:

            if response.status != 200:
                print(
                    f"Solana RPC HTTP {response.status}"
                )
                return None

            data = await response.json()

            if "error" in data:
                print(
                    f"Solana RPC error: {data['error']}"
                )
                return None

            return data.get("result")

    except Exception as exc:
        print(f"Solana RPC error: {exc}")
        return None


# ============================================================
# SOLANA SECURITY
# ============================================================

async def solana_risk_check(address):
    result = {
        "mint_authority": None,
        "freeze_authority": None,
        "top_holder_percent": None,
        "risks": [],
    }

    largest = await solana_rpc(
        "getTokenLargestAccounts",
        [address]
    )

    supply = await solana_rpc(
        "getTokenSupply",
        [address]
    )

    if largest and supply:
        try:
            total = float(
                supply["value"]["uiAmount"] or 0
            )

            if total > 0:
                amounts = [
                    float(
                        item.get("uiAmount") or 0
                    )
                    for item in largest.get(
                        "value",
                        []
                    )
                ]

                if amounts:
                    concentration = (
                        max(amounts) / total * 100
                    )

                    result[
                        "top_holder_percent"
                    ] = concentration

                    if concentration >= 50:
                        result["risks"].append(
                            f"Top Holder ca. "
                            f"{concentration:.1f}%"
                        )

                    elif concentration >= 25:
                        result["risks"].append(
                            f"Hohe Holder-Konzentration: "
                            f"{concentration:.1f}%"
                        )

        except Exception:
            pass

    account_info = await solana_rpc(
        "getAccountInfo",
        [
            address,
            {
                "encoding": "jsonParsed"
            }
        ]
    )

    if account_info:
        try:
            parsed = (
                account_info["value"]
                ["data"]
                ["parsed"]
                ["info"]
            )

            result["mint_authority"] = (
                parsed.get("mintAuthority")
            )

            result["freeze_authority"] = (
                parsed.get("freezeAuthority")
            )

            if result["mint_authority"]:
                result["risks"].append(
                    "Mint Authority aktiv"
                )

            if result["freeze_authority"]:
                result["risks"].append(
                    "Freeze Authority aktiv"
                )

        except Exception:
            pass

    return result


# ============================================================
# EARLY BUYER HEURISTIC
# ============================================================

async def early_buyer_snapshot(address):
    """
    Heuristische Analyse.

    Diese Funktion liefert einen Snapshot von
    Transaktionen rund um einen Token.

    Sie ist KEIN vollständiger On-Chain-Buyer-Indexer.
    """

    result = {
        "early_transactions": 0,
        "early_wallets": [],
    }

    signatures = await solana_rpc(
        "getSignaturesForAddress",
        [
            address,
            {
                "limit": 25
            }
        ]
    )

    if not signatures:
        return result

    result["early_transactions"] = len(
        signatures
    )

    wallets = []

    for signature in signatures[:10]:
        sig = signature.get("signature")

        if not sig:
            continue

        tx = await solana_rpc(
            "getTransaction",
            [
                sig,
                {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        )

        if not tx:
            continue

        try:
            keys = (
                tx["transaction"]
                ["message"]
                ["accountKeys"]
            )

            for key in keys:
                pubkey = key.get("pubkey")

                if (
                    pubkey
                    and pubkey not in wallets
                ):
                    wallets.append(pubkey)

        except Exception:
            continue

    result["early_wallets"] = wallets[:10]

    return result


# ============================================================
# TELEGRAM
# ============================================================

async def send_message(application, text):
    if not TELEGRAM_BOT_TOKEN:
        print(text)
        return

    if not ALLOWED_CHAT_ID:
        print(
            "ALLOWED_CHAT_ID fehlt. "
            "Telegram-Nachricht nicht gesendet."
        )
        return

    try:
        await application.bot.send_message(
            chat_id=ALLOWED_CHAT_ID,
            text=text,
            disable_web_page_preview=True
        )

    except Exception as exc:
        print(
            f"Telegram error: {exc}"
        )


def build_alert(
    pair,
    analysis,
    risk=None,
    early=None
):
    base = pair.get("baseToken") or {}

    symbol = (
        base.get("symbol")
        or "UNKNOWN"
    )

    name = (
        base.get("name")
        or symbol
    )

    address = (
        base.get("address")
        or ""
    )

    chain = (
        pair.get("chainId")
        or "unknown"
    )

    dex = (
        pair.get("dexId")
        or "unknown"
    )

    url = (
        pair.get("url")
        or ""
    )

    risks = []

    if analysis:
        risks.extend(
            analysis.get("risks", [])
        )

    if risk:
        risks.extend(
            risk.get("risks", [])
        )

    if not risks:
        risks.append(
            "Keine offensichtlichen "
            "Scanner-Risiken"
        )

    early_wallet_count = 0

    if early:
        early_wallet_count = len(
            early.get(
                "early_wallets",
                []
            )
        )

    return (
        "🚨 MEMECOIN ALERT\n\n"
        f"🪙 {name} ({symbol})\n"
        f"⛓️ Chain: {chain}\n"
        f"🏦 DEX: {dex}\n\n"
        f"⭐ Score: {analysis['score']}/100\n"
        f"💧 Liquidität: "
        f"{fmt_money(analysis['liquidity'])}\n"
        f"📊 Volumen 24h: "
        f"{fmt_money(analysis['volume'])}\n"
        f"🟢 Buys: {analysis['buys']}\n"
        f"🔴 Sells: {analysis['sells']}\n"
        f"📈 Change 24h: "
        f"{analysis['price_change']:.2f}%\n\n"
        f"👛 Frühe Wallet-Snapshot: "
        f"{early_wallet_count}\n\n"
        "⚠️ Risiken:\n"
        + "\n".join(
            f"• {item}"
            for item in risks
        )
        + "\n\n"
        f"📍 Token:\n{address}\n\n"
        f"🔗 DexScreener:\n{url}"
    )


# ============================================================
# TOKEN SCANNER
# ============================================================

async def scan_profiles(application):
    profiles = (
        await get_latest_token_profiles()
    )

    if not profiles:
        print(
            "Keine aktuellen Token-Profile gefunden."
        )

        return {
            "scanned": 0,
            "alerts_sent": 0,
            "candidates": [],
        }

    scanned = 0
    alerts_sent = 0
    candidates = []

    for profile in profiles[:30]:
        chain = profile.get("chainId")
        address = profile.get(
            "tokenAddress"
        )

        if not chain or not address:
            continue

        scanned += 1

        pairs = await get_token_pairs(
            chain,
            address
        )

        if not pairs:
            continue

        pairs.sort(
            key=lambda p: float(
                (p.get("liquidity") or {})
                .get("usd") or 0
            ),
            reverse=True
        )

        pair = pairs[0]

        analysis = analyze_pair(pair)

        # Kandidat für die manuelle Scan-Ausgabe
        # Nur Liquidität und Volumen müssen die
        # Mindestwerte erfüllen.
        if (
            analysis["liquidity"]
            >= MIN_LIQUIDITY_USD
            and
            analysis["volume"]
            >= MIN_VOLUME_24H
        ):
            candidates.append({
                "pair": pair,
                "analysis": analysis,
            })

        # Für einen Alert müssen zusätzlich
        # die Score-Anforderungen erfüllt sein.
        if (
            analysis["liquidity"]
            < MIN_LIQUIDITY_USD
        ):
            continue

        if (
            analysis["volume"]
            < MIN_VOLUME_24H
        ):
            continue

        if (
            analysis["score"]
            < ALERT_SCORE
        ):
            continue

        key = token_key(pair)

        last = last_alerts.get(
            key,
            0
        )

        if time.time() - last < 3600:
            continue

        last_alerts[key] = time.time()

        risk = None
        early = None

        if chain == "solana":
            risk = await solana_risk_check(
                address
            )

            early = await early_buyer_snapshot(
                address
            )

        alert_text = build_alert(
            pair,
            analysis,
            risk,
            early
        )

        conn = db_connect()

        conn.execute(
            """
            INSERT INTO alerts
            (
                chain,
                token_address,
                symbol,
                score,
                liquidity,
                volume,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chain,
                address,
                pair.get(
                    "baseToken",
                    {}
                ).get(
                    "symbol"
                ),
                analysis["score"],
                analysis["liquidity"],
                analysis["volume"],
                now_iso(),
            )
        )

        conn.commit()
        conn.close()

        await send_message(
            application,
            alert_text
        )

        alerts_sent += 1

    # Höchsten Score zuerst
    candidates.sort(
        key=lambda item: (
            item["analysis"]["score"],
            item["analysis"]["liquidity"],
            item["analysis"]["volume"],
        ),
        reverse=True
    )

    result = {
        "scanned": scanned,
        "alerts_sent": alerts_sent,
        "candidates": candidates[:5],
    }

    print(
        f"Scan abgeschlossen: "
        f"{scanned} Tokens geprüft, "
        f"{alerts_sent} Alerts, "
        f"{len(candidates)} Kandidaten."
    )

    return result


async def scanner_loop(application):
    print("Scanner gestartet.")

    while True:
        try:
            await scan_profiles(
                application
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            print(
                f"Scanner error: {exc}"
            )

        await asyncio.sleep(
            SCAN_INTERVAL
        )


# ============================================================
# PUMP.FUN WEBSOCKET
# ============================================================

async def pumpfun_listener(application):
    print(
        "Pump.fun WebSocket wird gestartet..."
    )

    while True:
        try:
            async with websockets.connect(
                SOLANA_WS,
                ping_interval=20,
                ping_timeout=20
            ) as websocket:

                subscription = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {
                            "mentions": [
                                PUMP_FUN_PROGRAM
                            ]
                        },
                        {
                            "commitment": "confirmed"
                        }
                    ]
                }

                await websocket.send(
                    json.dumps(subscription)
                )

                print(
                    "Pump.fun WebSocket verbunden."
                )

                async for raw_message in websocket:
                    try:
                        message = json.loads(
                            raw_message
                        )

                        value = (
                            message
                            .get("params", {})
                            .get("result", {})
                            .get("value", {})
                        )

                        logs = value.get(
                            "logs",
                            []
                        )

                        joined = (
                            " ".join(logs)
                            .lower()
                        )

                        if (
                            "createmint" in joined
                            or "initialize_mint"
                            in joined
                        ):
                            signature = (
                                value.get(
                                    "signature",
                                    "unknown"
                                )
                            )

                            print(
                                "Mögliche Pump.fun "
                                "Aktivität:",
                                signature
                            )

                    except Exception:
                        continue

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            print(
                f"WebSocket error: {exc}"
            )

            await asyncio.sleep(5)


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    if not allowed(update):
        return

    await update.message.reply_text(
        "🤖 Memecoin Scanner aktiv.\n\n"
        "/status – Bot-Status\n"
        "/scan – manuellen Scan starten\n"
        "/paper – Paper-Trading Übersicht\n"
        "/buy <chain> <token> <usd> "
        "– Paper Buy"
    )


async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    if not allowed(update):
        return

    await update.message.reply_text(
        "🟢 BOT STATUS\n\n"
        "Scanner: aktiv\n"
        "DexScreener: aktiv\n"
        "Paper Trading: aktiv\n"
        "Solana Analyse: aktiv\n"
        "Pump.fun Listener: aktiv"
    )


async def scan_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    if not allowed(update):
        return

    await update.message.reply_text(
        "🔎 Manueller Scan gestartet..."
    )

    try:
        result = await scan_profiles(
            context.application
        )

        message = (
            "✅ Scan abgeschlossen.\n\n"
            f"🔎 Geprüft: "
            f"{result['scanned']}\n"
            f"🚨 Alerts: "
            f"{result['alerts_sent']}\n"
        )

        candidates = result.get(
            "candidates",
            []
        )

        if candidates:
            message += (
                "\n🏆 TOP-KANDIDATEN:\n"
            )

            for i, candidate in enumerate(
                candidates,
                1
            ):
                pair = candidate["pair"]
                analysis = candidate[
                    "analysis"
                ]

                base = pair.get(
                    "baseToken"
                ) or {}

                name = (
                    base.get("name")
                    or "Unknown"
                )

                symbol = (
                    base.get("symbol")
                    or "?"
                )

                liquidity = analysis.get(
                    "liquidity",
                    0
                )

                volume = analysis.get(
                    "volume",
                    0
                )

                score = analysis.get(
                    "score",
                    0
                )

                buys = analysis.get(
                    "buys",
                    0
                )

                sells = analysis.get(
                    "sells",
                    0
                )

                change = analysis.get(
                    "price_change",
                    0
                )

                risks = analysis.get(
                    "risks",
                    []
                )

                message += (
                    f"\n{i}. "
                    f"{name} ({symbol})\n"
                    f"   ⛓️ Chain: "
                    f"{pair.get('chainId', '?')}\n"
                    f"   ⭐ Score: "
                    f"{score}/100\n"
                    f"   💧 Liquidität: "
                    f"{fmt_money(liquidity)}\n"
                    f"   📊 Volumen 24h: "
                    f"{fmt_money(volume)}\n"
                    f"   🟢 Käufe: "
                    f"{buys} | "
                    f"🔴 Verkäufe: "
                    f"{sells}\n"
                    f"   📈 24h: "
                    f"{change:.2f}%\n"
                )

                if risks:
                    message += (
                        "   ⚠️ Risiken: "
                        + ", ".join(risks)
                        + "\n"
                    )
                else:
                    message += (
                        "   ⚠️ Risiken: "
                        "Keine offensichtlichen\n"
                    )

                url = pair.get("url")

                if url:
                    message += (
                        f"   🔗 {url}\n"
                    )

        else:
            message += (
                "\n❌ Keine passenden "
                "Kandidaten gefunden."
            )

        # Telegram-Nachrichten haben eine maximale
        # Nachrichtenlänge. Deshalb wird die Ausgabe
        # bei Bedarf aufgeteilt.
        max_length = 4000

        for start in range(
            0,
            len(message),
            max_length
        ):
            await update.message.reply_text(
                message[
                    start:start + max_length
                ],
                disable_web_page_preview=True
            )

    except Exception as exc:
        await update.message.reply_text(
            f"❌ Scan-Fehler: {exc}"
        )


async def paper_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    if not allowed(update):
        return

    conn = db_connect()

    positions = conn.execute(
        """
        SELECT *
        FROM paper_positions
        WHERE closed_at IS NULL
        ORDER BY id DESC
        """
    ).fetchall()

    conn.close()

    if not positions:
        await update.message.reply_text(
            "🧪 Keine offenen "
            "Paper-Positionen."
        )
        return

    lines = [
        "🧪 OFFENE "
        "PAPER-POSITIONEN\n"
    ]

    for position in positions:
        lines.append(
            f"• {position['symbol']} "
            f"({position['chain']})\n"
            f"  Einsatz: "
            f"${position['amount_usd']:.2f}\n"
            f"  Entry: "
            f"{fmt_price(position['entry_price'])}"
        )

    await update.message.reply_text(
        "\n".join(lines)
    )


async def buy_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):
    if not allowed(update):
        return

    args = context.args

    if len(args) != 3:
        await update.message.reply_text(
            "Verwendung:\n"
            "/buy <chain> <token> <usd>\n\n"
            "Beispiel:\n"
            "/buy solana TOKENADRESSE 10"
        )
        return

    chain = args[0]
    address = args[1]

    try:
        amount = float(args[2])

    except ValueError:
        await update.message.reply_text(
            "❌ USD-Betrag ist ungültig."
        )
        return

    if amount <= 0:
        await update.message.reply_text(
            "❌ Betrag muss größer als 0 sein."
        )
        return

    pairs = await get_token_pairs(
        chain,
        address
    )

    if not pairs:
        await update.message.reply_text(
            "❌ Kein handelbares Pair gefunden."
        )
        return

    pairs.sort(
        key=lambda p: float(
            (p.get("liquidity") or {})
            .get("usd") or 0
        ),
        reverse=True
    )

    pair = pairs[0]

    price = float(
        pair.get("priceUsd") or 0
    )

    if price <= 0:
        await update.message.reply_text(
            "❌ Kein gültiger Preis verfügbar."
        )
        return

    symbol = (
        pair.get("baseToken", {})
        .get("symbol", "UNKNOWN")
    )

    conn = db_connect()

    conn.execute(
        """
        INSERT INTO paper_positions
        (
            chain,
            token_address,
            symbol,
            entry_price,
            amount_usd,
            opened_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            chain,
            address,
            symbol,
            price,
            amount,
            now_iso(),
        )
    )

    conn.commit()
    conn.close()

    await update.message.reply_text(
        "🧪 PAPER BUY\n\n"
        f"Token: {symbol}\n"
        f"Chain: {chain}\n"
        f"Einsatz: ${amount:.2f}\n"
        f"Entry: {fmt_price(price)}\n\n"
        "⚠️ Kein echter Kauf."
    )


# ============================================================
# APPLICATION
# ============================================================

async def post_init(application):
    global scan_task
    global ws_task

    init_db()

    scan_task = asyncio.create_task(
        scanner_loop(application)
    )

    ws_task = asyncio.create_task(
        pumpfun_listener(application)
    )


async def post_shutdown(application):
    global scan_task
    global ws_task
    global http_session

    for task in (
        scan_task,
        ws_task
    ):
        if task:
            task.cancel()

    tasks = [
        task
        for task in (
            scan_task,
            ws_task
        )
        if task
    ]

    if tasks:
        await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

    scan_task = None
    ws_task = None

    if http_session:
        await http_session.close()
        http_session = None


def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN fehlt."
        )

    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start_command
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            status_command
        )
    )

    application.add_handler(
        CommandHandler(
            "scan",
            scan_command
        )
    )

    application.add_handler(
        CommandHandler(
            "paper",
            paper_command
        )
    )

    application.add_handler(
        CommandHandler(
            "buy",
            buy_command
        )
    )

    print(
        "Memecoin Scanner wird gestartet..."
    )

    application.run_polling()


if __name__ == "__main__":
    main()