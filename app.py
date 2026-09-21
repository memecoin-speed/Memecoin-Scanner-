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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_CHAT_ID = os.getenv("ALLOWED_CHAT_ID", "")

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


# ============================================================
# GLOBALS
# ============================================================

http_session = None
scan_task = None
ws_task = None

last_alerts = {}

ALERT_COOLDOWN = 30 * 60


# ============================================================
# DATABASE
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_FILE)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT,
            symbol TEXT,
            price REAL,
            amount REAL,
            timestamp INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT,
            symbol TEXT,
            score INTEGER,
            timestamp INTEGER
        )
    """)

    conn.commit()
    conn.close()


# ============================================================
# HTTP
# ============================================================

async def get_json(url, params=None):
    global http_session

    try:
        if http_session is None:
            http_session = aiohttp.ClientSession()

        async with http_session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as response:

            if response.status != 200:
                return None

            return await response.json()

    except Exception as e:
        print("HTTP ERROR:", e)
        return None


# ============================================================
# SOLANA RPC
# ============================================================

async def solana_rpc(method, params):
    global http_session

    if http_session is None:
        http_session = aiohttp.ClientSession()

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }

    try:
        async with http_session.post(
            SOLANA_RPC,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as response:

            if response.status != 200:
                return {}

            return await response.json()

    except Exception as e:
        print("SOLANA RPC ERROR:", e)
        return {}


# ============================================================
# DEXSCREENER
# ============================================================

async def get_latest_token_profiles():
    """
    Holt aktuelle Token/Pairs über die DexScreener Search API.

    Die Chain-Auswahl bleibt bewusst offen.
    """

    data = await get_json(
        f"{DEXSCREENER_API}/latest/dex/search",
        params={
            "q": "SOL"
        }
    )

    if not isinstance(data, dict):
        return []

    pairs = data.get("pairs") or []

    profiles = []
    seen = set()

    for pair in pairs:

        base = pair.get("baseToken") or {}

        address = base.get("address")

        if not address:
            continue

        chain = pair.get("chainId")

        key = f"{chain}:{address}"

        if key in seen:
            continue

        seen.add(key)

        profiles.append({
            "chainId": chain,
            "tokenAddress": address,
        })

    return profiles


async def get_token_pairs(chain, address):
    """
    Holt alle DexScreener-Pairs eines Tokens.
    """

    data = await get_json(
        f"{DEXSCREENER_API}/latest/dex/tokens/{address}"
    )

    if not isinstance(data, dict):
        return []

    pairs = data.get("pairs") or []

    filtered = []

    for pair in pairs:

        if pair.get("chainId") != chain:
            continue

        filtered.append(pair)

    return filtered


# ============================================================
# SCORING
# ============================================================

def calculate_score(pair):

    score = 0

    liquidity = (
        pair.get("liquidity", {})
        .get("usd", 0)
        or 0
    )

    volume = (
        pair.get("volume", {})
        .get("h24", 0)
        or 0
    )

    txns = (
        pair.get("txns", {})
        .get("h24", {})
    )

    buys = txns.get("buys", 0) or 0
    sells = txns.get("sells", 0) or 0

    price_change = (
        pair.get("priceChange", {})
        .get("h24", 0)
        or 0
    )

    pair_created = pair.get(
        "pairCreatedAt"
    )

    # --------------------------------------------------------
    # LIQUIDITY
    # --------------------------------------------------------

    if liquidity >= 100000:
        score += 25

    elif liquidity >= 50000:
        score += 20

    elif liquidity >= 20000:
        score += 15

    elif liquidity >= 10000:
        score += 10

    elif liquidity >= 5000:
        score += 5

    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    if volume >= 1000000:
        score += 25

    elif volume >= 500000:
        score += 20

    elif volume >= 100000:
        score += 15

    elif volume >= 50000:
        score += 10

    elif volume >= 5000:
        score += 5

    # --------------------------------------------------------
    # BUY / SELL RATIO
    # --------------------------------------------------------

    total_transactions = buys + sells

    if total_transactions > 0:

        buy_ratio = buys / total_transactions

        if buy_ratio >= 0.75:
            score += 20

        elif buy_ratio >= 0.65:
            score += 15

        elif buy_ratio >= 0.55:
            score += 10

        elif buy_ratio >= 0.50:
            score += 5

    # --------------------------------------------------------
    # PRICE MOMENTUM
    # --------------------------------------------------------

    if 5 <= price_change <= 50:
        score += 15

    elif 50 < price_change <= 100:
        score += 10

    elif 100 < price_change <= 200:
        score += 5

    elif price_change > 500:
        score -= 15

    elif price_change > 200:
        score -= 10

    # --------------------------------------------------------
    # PAIR AGE
    # --------------------------------------------------------

    if pair_created:

        try:
            age_hours = (
                time.time() * 1000 - pair_created
            ) / 1000 / 3600

            if age_hours <= 1:
                score += 15

            elif age_hours <= 6:
                score += 12

            elif age_hours <= 24:
                score += 8

            elif age_hours <= 72:
                score += 4

        except Exception:
            pass

    return max(
        0,
        min(100, score)
    )


# ============================================================
# BASIC PAIR ANALYSIS
# ============================================================

def analyze_pair(pair):

    liquidity = (
        pair.get("liquidity", {})
        .get("usd", 0)
        or 0
    )

    volume = (
        pair.get("volume", {})
        .get("h24", 0)
        or 0
    )

    txns = (
        pair.get("txns", {})
        .get("h24", {})
    )

    buys = txns.get("buys", 0) or 0
    sells = txns.get("sells", 0) or 0

    price_change = (
        pair.get("priceChange", {})
        .get("h24", 0)
        or 0
    )

    risks = []

    if liquidity < MIN_LIQUIDITY_USD:
        risks.append("Geringe Liquidität")

    if volume < MIN_VOLUME_24H:
        risks.append("Geringes Volumen")

    if sells > buys:
        risks.append("Mehr Verkäufe als Käufe")

    if price_change > 200:
        risks.append("Extremer Preisanstieg")

    if price_change < -50:
        risks.append("Starker Preisverlust")

    score = calculate_score(pair)

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
# SOLANA RISK ANALYSIS
# ============================================================

async def solana_risk_check(address):
    """
    On-Chain Risikoanalyse für Solana Tokens.

    Prüft:
    - Mint Authority
    - Freeze Authority
    - Token Supply
    - größte Token-Accounts
    - Holder-Konzentration
    """

    result = {
        "risk_level": "unknown",
        "warnings": [],
        "mint_authority": None,
        "freeze_authority": None,
        "supply": 0,
        "top_holder_share": 0,
    }

    try:

        # ----------------------------------------------------
        # 1. Mint Account
        # ----------------------------------------------------

        mint_data = await solana_rpc(
            "getAccountInfo",
            [
                address,
                {
                    "encoding": "jsonParsed"
                }
            ]
        )

        value = (
            mint_data
            .get("result", {})
            .get("value")
        )

        if not value:

            result["warnings"].append(
                "Mint Account nicht gefunden"
            )

            result["risk_level"] = "high"

            return result

        parsed = (
            value
            .get("data", {})
            .get("parsed", {})
        )

        info = parsed.get(
            "info",
            {}
        )

        # ----------------------------------------------------
        # 2. Authorities
        # ----------------------------------------------------

        result["mint_authority"] = info.get(
            "mintAuthority"
        )

        result["freeze_authority"] = info.get(
            "freezeAuthority"
        )

        result["supply"] = int(
            info.get(
                "supply",
                0
            )
        )

        # ----------------------------------------------------
        # 3. Mint Authority
        # ----------------------------------------------------

        if result["mint_authority"]:

            result["warnings"].append(
                "Mint Authority aktiv"
            )

        # ----------------------------------------------------
        # 4. Freeze Authority
        # ----------------------------------------------------

        if result["freeze_authority"]:

            result["warnings"].append(
                "Freeze Authority aktiv"
            )

        # ----------------------------------------------------
        # 5. Largest Token Accounts
        # ----------------------------------------------------

        largest = await solana_rpc(
            "getTokenLargestAccounts",
            [address]
        )

        accounts = (
            largest
            .get("result", {})
            .get("value", [])
        )

        supply = result["supply"]

        if supply > 0 and accounts:

            top_amount = 0

            for account in accounts[:10]:

                amount = int(
                    account.get(
                        "amount",
                        0
                    )
                )

                top_amount += amount

            share = (
                top_amount / supply
            ) * 100

            result["top_holder_share"] = round(
                share,
                2
            )

            # ------------------------------------------------
            # Holder concentration
            # ------------------------------------------------

            if share >= 80:

                result["warnings"].append(
                    f"Sehr hohe Holder-Konzentration: {share:.1f}%"
                )

            elif share >= 60:

                result["warnings"].append(
                    f"Hohe Holder-Konzentration: {share:.1f}%"
                )

            elif share >= 40:

                result["warnings"].append(
                    f"Erhöhte Holder-Konzentration: {share:.1f}%"
                )

        # ----------------------------------------------------
        # 6. Risk Level
        # ----------------------------------------------------

        warning_count = len(
            result["warnings"]
        )

        if warning_count == 0:

            result["risk_level"] = "low"

        elif warning_count == 1:

            result["risk_level"] = "medium"

        else:

            result["risk_level"] = "high"

        return result

    except Exception as e:

        result["risk_level"] = "unknown"

        result["warnings"].append(
            f"Risikoanalyse fehlgeschlagen: {str(e)[:120]}"
        )

        return result


# ============================================================
# EARLY BUYER SNAPSHOT
# ============================================================

async def early_buyer_snapshot(address):

    """
    Aktuell nur heuristischer Snapshot.

    Kein vollständiger Early-Buyer-Detektor.
    """

    try:

        data = await solana_rpc(
            "getSignaturesForAddress",
            [
                address,
                {
                    "limit": 10
                }
            ]
        )

        signatures = (
            data
            .get("result", [])
        )

        wallets = []

        for signature in signatures:

            sig = signature.get(
                "signature"
            )

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

            value = (
                tx
                .get("result")
            )

            if not value:
                continue

            message = (
                value
                .get("transaction", {})
                .get("message", {})
            )

            account_keys = (
                message.get(
                    "accountKeys",
                    []
                )
            )

            for account in account_keys:

                if isinstance(account, dict):

                    pubkey = account.get(
                        "pubkey"
                    )

                else:

                    pubkey = account

                if pubkey and pubkey not in wallets:

                    wallets.append(pubkey)

        return wallets[:10]

    except Exception as e:

        print(
            "EARLY BUYER ERROR:",
            e
        )

        return []


# ============================================================
# FORMAT RISK
# ============================================================

def format_risk(risk):

    if not risk:

        return "⚪ Risikoanalyse nicht verfügbar"

    level = risk.get(
        "risk_level",
        "unknown"
    )

    warnings = risk.get(
        "warnings",
        []
    )

    if level == "low":

        icon = "🟢"

    elif level == "medium":

        icon = "🟡"

    elif level == "high":

        icon = "🔴"

    else:

        icon = "⚪"

    if not warnings:

        return (
            f"{icon} Risiko: niedrig\n"
            "Keine offensichtlichen On-Chain-Risiken"
        )

    text = (
        f"{icon} Risiko: {level.upper()}\n"
    )

    for warning in warnings:

        text += f"⚠️ {warning}\n"

    return text.rstrip()


# ============================================================
# ALERT
# ============================================================

async def send_alert(
    application,
    pair,
    analysis,
    risk=None
):

    address = (
        pair
        .get("baseToken", {})
        .get("address", "")
    )

    symbol = (
        pair
        .get("baseToken", {})
        .get("symbol", "?")
    )

    name = (
        pair
        .get("baseToken", {})
        .get("name", symbol)
    )

    score = analysis["score"]

    now = time.time()

    last = last_alerts.get(
        address,
        0
    )

    if now - last < ALERT_COOLDOWN:

        return

    last_alerts[address] = now

    risk_text = format_risk(
        risk
    )

    message = f"""
🚨 MEMECOIN ALERT

🪙 {name} ({symbol})

⛓️ Chain: {pair.get("chainId", "?")}

⭐ Score: {score}/100

💧 Liquidität:
${analysis["liquidity"]:,.0f}

📊 Volumen 24h:
${analysis["volume"]:,.0f}

🟢 Käufe:
{analysis["buys"]}

🔴 Verkäufe:
{analysis["sells"]}

📈 24h:
{analysis["price_change"]:.2f}%

{risk_text}

🔗 {pair.get("url", "")}
""".strip()

    try:

        await application.bot.send_message(
            chat_id=ALLOWED_CHAT_ID,
            text=message
        )

        conn = sqlite3.connect(
            DB_FILE
        )

        conn.execute(
            """
            INSERT INTO alerts
            (token, symbol, score, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (
                address,
                symbol,
                score,
                int(time.time())
            )
        )

        conn.commit()
        conn.close()

    except Exception as e:

        print(
            "ALERT ERROR:",
            e
        )


# ============================================================
# SCANNER
# ============================================================

async def perform_scan(application=None):

    profiles = await get_latest_token_profiles()

    candidates = []

    checked = 0

    for profile in profiles:

        chain = profile.get(
            "chainId"
        )

        address = profile.get(
            "tokenAddress"
        )

        if not chain or not address:
            continue

        pairs = await get_token_pairs(
            chain,
            address
        )

        if not pairs:
            continue

        # Bestes Pair anhand Liquidität
        pairs.sort(
            key=lambda p:
            (
                p.get("liquidity", {})
                .get("usd", 0)
                or 0
            ),
            reverse=True
        )

        pair = pairs[0]

        checked += 1

        analysis = analyze_pair(
            pair
        )

        # Mindestanforderungen
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

        # ----------------------------------------------------
        # Risikoanalyse für Kandidaten
        # ----------------------------------------------------

        risk = None

        if chain == "solana":

            risk = await solana_risk_check(
                address
            )

        candidates.append({
            "pair": pair,
            "analysis": analysis,
            "risk": risk,
        })

        # ----------------------------------------------------
        # Alerts
        # ----------------------------------------------------

        if (
            application
            and analysis["score"]
            >= ALERT_SCORE
        ):

            await send_alert(
                application,
                pair,
                analysis,
                risk
            )

        # Nicht zu viele RPC-Anfragen
        if len(candidates) >= 10:
            break

    candidates.sort(
        key=lambda x:
        x["analysis"]["score"],
        reverse=True
    )

    return checked, candidates[:5]


# ============================================================
# TELEGRAM HELPERS
# ============================================================

def authorized(update):

    if not ALLOWED_CHAT_ID:
        return True

    if not update.effective_chat:
        return False

    return str(
        update.effective_chat.id
    ) == str(ALLOWED_CHAT_ID)


# ============================================================
# /START
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        return

    await update.message.reply_text(
        """
🤖 Memecoin Scanner

Verfügbare Befehle:

/status
/scan
/paper
/buy <token> <symbol> <price> <amount>

Scanner:
DexScreener + Solana Analyse
Paper Trading aktiv
""".strip()
    )


# ============================================================
# /STATUS
# ============================================================

async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        return

    await update.message.reply_text(
        """
🟢 BOT STATUS

Scanner: aktiv
DexScreener: aktiv
Paper Trading: aktiv
Solana Analyse: aktiv
Pump.fun Listener: aktiv
Risikoanalyse: aktiv
""".strip()
    )


# ============================================================
# /SCAN
# ============================================================

async def scan_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        return

    await update.message.reply_text(
        "🔎 Starte Scan..."
    )

    checked, candidates = await perform_scan()

    if not candidates:

        await update.message.reply_text(
            f"""
✅ Scan abgeschlossen.

🔎 Geprüft: {checked}
🚨 Alerts: 0

❌ Keine passenden Kandidaten gefunden.
""".strip()
        )

        return

    text = f"""
✅ Scan abgeschlossen.

🔎 Geprüft: {checked}
🚨 Kandidaten: {len(candidates)}

🏆 TOP-KANDIDATEN:
""".strip()

    for index, candidate in enumerate(
        candidates,
        start=1
    ):

        pair = candidate["pair"]
        analysis = candidate["analysis"]
        risk = candidate["risk"]

        base = pair.get(
            "baseToken",
            {}
        )

        name = base.get(
            "name",
            "Unknown"
        )

        symbol = base.get(
            "symbol",
            "?"
        )

        chain = pair.get(
            "chainId",
            "?"
        )

        text += f"""

{index}. {name} ({symbol})
   ⛓️ Chain: {chain}
   ⭐ Score: {analysis["score"]}/100
   💧 Liquidität: ${analysis["liquidity"]:,.1f}
   📊 Volumen 24h: ${analysis["volume"]:,.1f}
   🟢 Käufe: {analysis["buys"]} | 🔴 Verkäufe: {analysis["sells"]}
   📈 24h: {analysis["price_change"]:.2f}%
"""

        if analysis["risks"]:

            text += (
                "   ⚠️ Markt-Risiken: "
                + ", ".join(
                    analysis["risks"]
                )
                + "\n"
            )

        if risk:

            risk_level = risk.get(
                "risk_level",
                "unknown"
            )

            text += (
                f"   🔐 On-Chain Risiko: "
                f"{risk_level}\n"
            )

            warnings = risk.get(
                "warnings",
                []
            )

            for warning in warnings[:3]:

                text += (
                    f"   ⚠️ {warning}\n"
                )

        else:

            text += (
                "   🔐 On-Chain Risiko: "
                "nicht verfügbar\n"
            )

        text += (
            f"   🔗 {pair.get('url', '')}\n"
        )

    await update.message.reply_text(
        text.strip(),
        disable_web_page_preview=True
    )


# ============================================================
# PAPER TRADING
# ============================================================

async def paper_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        return

    conn = sqlite3.connect(
        DB_FILE
    )

    rows = conn.execute(
        """
        SELECT token, symbol, price,
               amount, timestamp
        FROM paper_positions
        ORDER BY timestamp DESC
        LIMIT 10
        """
    ).fetchall()

    conn.close()

    if not rows:

        await update.message.reply_text(
            "📊 Keine Paper-Trading-Positionen."
        )

        return

    text = "📊 PAPER TRADING\n"

    for row in rows:

        token, symbol, price, amount, timestamp = row

        text += f"""

🪙 {symbol}
Preis: ${price}
Menge: {amount}
"""

    await update.message.reply_text(
        text.strip()
    )


# ============================================================
# /BUY
# ============================================================

async def buy_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not authorized(update):
        return

    args = context.args

    if len(args) != 4:

        await update.message.reply_text(
            """
Verwendung:

/buy <token> <symbol> <price> <amount>

Beispiel:

/buy ABC123 ABC 0.00001 1000000

⚠️ Nur Paper Trading.
Keine echte Transaktion.
""".strip()
        )

        return

    token = args[0]
    symbol = args[1]

    try:

        price = float(
            args[2]
        )

        amount = float(
            args[3]
        )

    except ValueError:

        await update.message.reply_text(
            "❌ Preis und Menge müssen Zahlen sein."
        )

        return

    conn = sqlite3.connect(
        DB_FILE
    )

    conn.execute(
        """
        INSERT INTO paper_positions
        (token, symbol, price, amount, timestamp)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            token,
            symbol,
            price,
            amount,
            int(time.time())
        )
    )

    conn.commit()
    conn.close()

    await update.message.reply_text(
        f"""
📝 PAPER BUY

🪙 {symbol}
💰 Preis: ${price}
📦 Menge: {amount}

✅ Position gespeichert.

⚠️ Keine echte Transaktion.
""".strip()
    )


# ============================================================
# AUTOMATIC SCANNER
# ============================================================

async def scanner_loop(
    application
):

    while True:

        try:

            await perform_scan(
                application
            )

        except Exception as e:

            print(
                "SCANNER ERROR:",
                e
            )

        await asyncio.sleep(
            SCAN_INTERVAL
        )


# ============================================================
# PUMP.FUN LISTENER
# ============================================================

async def pumpfun_listener():

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
                                "6EF8rrecthR5Dkzon8Nwu78hRvf8kT3qK7Q7c9R5Qn"
                            ]
                        },
                        {
                            "commitment": "confirmed"
                        }
                    ]
                }

                await websocket.send(
                    json.dumps(
                        subscription
                    )
                )

                while True:

                    message = await websocket.recv()

                    # Listener läuft bewusst nur
                    # als Detection-Layer.
                    # Kein automatischer Trade.

                    if message:
                        pass

        except Exception as e:

            print(
                "PUMP.FUN WS ERROR:",
                e
            )

            await asyncio.sleep(10)


# ============================================================
# POST INIT
# ============================================================

async def post_init(
    application
):

    global scan_task
    global ws_task

    init_db()

    scan_task = asyncio.create_task(
        scanner_loop(
            application
        )
    )

    ws_task = asyncio.create_task(
        pumpfun_listener()
    )

    print(
        "Scanner gestartet."
    )


# ============================================================
# POST SHUTDOWN
# ============================================================

async def post_shutdown(
    application
):

    global http_session
    global scan_task
    global ws_task

    if scan_task:

        scan_task.cancel()

    if ws_task:

        ws_task.cancel()

    if http_session:

        await http_session.close()

        http_session = None

    print(
        "Scanner beendet."
    )


# ============================================================
# MAIN
# ============================================================

def main():

    if not TELEGRAM_BOT_TOKEN:

        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN fehlt."
        )

    application = (
        Application.builder()
        .token(
            TELEGRAM_BOT_TOKEN
        )
        .post_init(
            post_init
        )
        .post_shutdown(
            post_shutdown
        )
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
        "Bot läuft..."
    )

    application.run_polling()


if __name__ == "__main__":
    main()