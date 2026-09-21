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
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_CHAT_ID = os.getenv("ALLOWED_CHAT_ID", "")

SOLANA_RPC = os.getenv(
    "SOLANA_RPC",
    "https://api.mainnet-beta.solana.com"
)

SOLANA_WS = os.getenv(
    "SOLANA_WS",
    "wss://api.mainnet-beta.solana.com"
)

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "30"))
MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "5000"))
MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H", "5000"))
ALERT_SCORE = float(os.getenv("ALERT_SCORE", "65"))
DB_FILE = os.getenv("DB_FILE", "scanner.db")

# ============================================================
# VERSION
# ============================================================

APP_VERSION = "3.0-multichain-memecoin-scanner"

# ============================================================
# KNOWN BASE / STABLE SYMBOLS
# ============================================================

BLOCKED_SYMBOLS = {
    "SOL",
    "WSOL",
    "USDC",
    "USDT",
    "DAI",
    "WETH",
    "WBTC",
    "WBNB",
    "WMATIC",
    "WAVAX",
    "WFTM",
    "WBTC",
    "BTC",
    "ETH",
}

# ============================================================
# DATABASE
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_FILE)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain TEXT,
            token_address TEXT,
            symbol TEXT,
            score REAL,
            created_at TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain TEXT,
            token_address TEXT,
            symbol TEXT,
            side TEXT,
            price REAL,
            amount_usd REAL,
            created_at TEXT
        )
    """)

    conn.commit()
    conn.close()


# ============================================================
# HELPERS
# ============================================================

async def get_json(session, url, params=None):
    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as response:

            if response.status != 200:
                return None

            return await response.json()

    except Exception:
        return None


async def post_json(session, url, payload):
    try:
        async with session.post(
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as response:

            if response.status != 200:
                return None

            return await response.json()

    except Exception:
        return None


def safe_float(value, default=0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def is_blocked_token(symbol):
    if not symbol:
        return True

    return symbol.upper().strip() in BLOCKED_SYMBOLS


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ============================================================
# SOLANA RPC
# ============================================================

async def solana_rpc(session, method, params=None):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params or []
    }

    return await post_json(
        session,
        SOLANA_RPC,
        payload
    )


# ============================================================
# TOKEN DISCOVERY
# ============================================================

async def get_latest_token_profiles(session):

    """
    DexScreener liefert über verschiedene Suchbegriffe
    Kandidaten. Wir kombinieren mehrere Suchabfragen,
    damit nicht nur SOL gefunden wird.
    """

    queries = [
        "meme",
        "pump",
        "dog",
        "cat",
        "pepe",
        "ai",
    ]

    all_pairs = []

    for query in queries:

        data = await get_json(
            session,
            "https://api.dexscreener.com/latest/dex/search",
            {"q": query}
        )

        if not data:
            continue

        pairs = data.get("pairs", [])

        if isinstance(pairs, list):
            all_pairs.extend(pairs)

    # --------------------------------------------------------
    # Filter
    # --------------------------------------------------------

    candidates = {}

    for pair in all_pairs:

        if not isinstance(pair, dict):
            continue

        chain = pair.get("chainId")
        base = pair.get("baseToken") or {}

        address = base.get("address")
        symbol = base.get("symbol")
        name = base.get("name")

        if not chain or not address:
            continue

        if is_blocked_token(symbol):
            continue

        liquidity = (
            pair.get("liquidity") or {}
        )

        liquidity_usd = safe_float(
            liquidity.get("usd")
        )

        volume = (
            pair.get("volume") or {}
        )

        volume_24h = safe_float(
            volume.get("h24")
        )

        if liquidity_usd < MIN_LIQUIDITY_USD:
            continue

        if volume_24h < MIN_VOLUME_24H:
            continue

        key = f"{chain}:{address}"

        # Beste Pair-Liquidität behalten
        if key not in candidates:

            candidates[key] = pair

        else:

            old_liquidity = safe_float(
                (candidates[key].get("liquidity") or {}).get("usd")
            )

            if liquidity_usd > old_liquidity:
                candidates[key] = pair

    return list(candidates.values())


# ============================================================
# SCORE
# ============================================================

def calculate_score(pair):

    liquidity = safe_float(
        (pair.get("liquidity") or {}).get("usd")
    )

    volume = safe_float(
        (pair.get("volume") or {}).get("h24")
    )

    txns = pair.get("txns") or {}
    h24 = txns.get("h24") or {}

    buys = safe_int(h24.get("buys"))
    sells = safe_int(h24.get("sells"))

    price_change = safe_float(
        (pair.get("priceChange") or {}).get("h24")
    )

    score = 0

    # --------------------------------------------------------
    # Liquidität
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
    # Volumen
    # --------------------------------------------------------

    if volume >= 500000:
        score += 25
    elif volume >= 100000:
        score += 20
    elif volume >= 50000:
        score += 15
    elif volume >= 10000:
        score += 10
    else:
        score += 5

    # --------------------------------------------------------
    # Buy/Sell-Verhältnis
    # --------------------------------------------------------

    total_txns = buys + sells

    if total_txns > 0:

        buy_ratio = buys / total_txns

        if buy_ratio >= 0.65:
            score += 25
        elif buy_ratio >= 0.55:
            score += 20
        elif buy_ratio >= 0.50:
            score += 15
        elif buy_ratio >= 0.40:
            score += 10
        else:
            score += 5

    # --------------------------------------------------------
    # Preisbewegung
    # --------------------------------------------------------

    if 5 <= price_change <= 50:
        score += 15

    elif 0 <= price_change < 5:
        score += 10

    elif 50 < price_change <= 100:
        score += 8

    elif price_change > 100:
        score -= 10

    elif price_change < -30:
        score -= 10

    return max(0, min(100, score))


# ============================================================
# MARKET RISK
# ============================================================

def analyze_market_risk(pair):

    risks = []

    liquidity = safe_float(
        (pair.get("liquidity") or {}).get("usd")
    )

    volume = safe_float(
        (pair.get("volume") or {}).get("h24")
    )

    price_change = safe_float(
        (pair.get("priceChange") or {}).get("h24")
    )

    txns = pair.get("txns") or {}
    h24 = txns.get("h24") or {}

    buys = safe_int(h24.get("buys"))
    sells = safe_int(h24.get("sells"))

    if liquidity < 10000:
        risks.append("Niedrige Liquidität")

    if volume < 10000:
        risks.append("Niedriges Volumen")

    if buys + sells > 0:

        buy_ratio = buys / (buys + sells)

        if buy_ratio < 0.40:
            risks.append("Verkaufsdruck")

    if price_change > 100:
        risks.append("Extremer Preisanstieg")

    if price_change < -30:
        risks.append("Starker Preisverlust")

    if not risks:
        risks.append("Keine offensichtlichen")

    return risks


# ============================================================
# SOLANA ON-CHAIN RISK
# ============================================================

async def solana_risk_check(session, token_address):

    result = {
        "risk": "UNKNOWN",
        "warnings": [],
        "mint_authority": None,
        "freeze_authority": None,
        "supply": 0,
        "top10_share": 0,
    }

    try:

        mint_result = await solana_rpc(
            session,
            "getAccountInfo",
            [
                token_address,
                {
                    "encoding": "jsonParsed"
                }
            ]
        )

        if not mint_result:
            result["warnings"].append(
                "Mint-Daten nicht verfügbar"
            )
            return result

        account = (
            mint_result
            .get("result", {})
            .get("value")
        )

        if not account:
            result["warnings"].append(
                "Token-Mint nicht gefunden"
            )
            return result

        parsed = (
            account
            .get("data", {})
            .get("parsed", {})
        )

        info = parsed.get("info", {})

        result["mint_authority"] = info.get(
            "mintAuthority"
        )

        result["freeze_authority"] = info.get(
            "freezeAuthority"
        )

        supply_info = info.get(
            "supply"
        )

        result["supply"] = safe_float(
            supply_info
        )

        # ----------------------------------------------------
        # Mint Authority
        # ----------------------------------------------------

        if result["mint_authority"]:
            result["warnings"].append(
                "Mint Authority aktiv"
            )

        # ----------------------------------------------------
        # Freeze Authority
        # ----------------------------------------------------

        if result["freeze_authority"]:
            result["warnings"].append(
                "Freeze Authority aktiv"
            )

        # ----------------------------------------------------
        # Largest Token Accounts
        # ----------------------------------------------------

        largest = await solana_rpc(
            session,
            "getTokenLargestAccounts",
            [
                token_address
            ]
        )

        if largest:

            accounts = (
                largest
                .get("result", {})
                .get("value", [])
            )

            total = result["supply"]

            if total > 0 and accounts:

                top_amount = 0

                for account in accounts[:10]:

                    amount = safe_float(
                        account.get("amount")
                    )

                    top_amount += amount

                share = (
                    top_amount / total
                ) * 100

                result["top10_share"] = share

                if share >= 80:
                    result["warnings"].append(
                        "Sehr hohe Top-10-Konzentration"
                    )

                elif share >= 60:
                    result["warnings"].append(
                        "Hohe Top-10-Konzentration"
                    )

        # ----------------------------------------------------
        # Risk Level
        # ----------------------------------------------------

        warning_count = len(
            result["warnings"]
        )

        if warning_count == 0:
            result["risk"] = "LOW"

        elif warning_count == 1:
            result["risk"] = "MEDIUM"

        else:
            result["risk"] = "HIGH"

        return result

    except Exception as e:

        result["warnings"].append(
            "On-Chain-Prüfung fehlgeschlagen"
        )

        return result


# ============================================================
# RISK FORMAT
# ============================================================

def format_risk(risk):

    level = risk.get("risk", "UNKNOWN")

    if level == "LOW":
        return "🟢 Niedrig"

    if level == "MEDIUM":
        return "🟡 Mittel"

    if level == "HIGH":
        return "🔴 Hoch"

    return "⚪ Unbekannt"


# ============================================================
# EARLY BUYER SNAPSHOT
# ============================================================

async def early_buyer_snapshot(session, chain, pair):

    """
    Aktuell bewusst nur Snapshot.
    Keine echte Gewinnprognose und kein automatischer Kauf.
    """

    result = {
        "available": False,
        "buyers": [],
    }

    if chain != "solana":
        return result

    try:

        pair_address = pair.get(
            "pairAddress"
        )

        if not pair_address:
            return result

        tx_result = await solana_rpc(
            session,
            "getSignaturesForAddress",
            [
                pair_address,
                {
                    "limit": 10
                }
            ]
        )

        if not tx_result:
            return result

        signatures = (
            tx_result
            .get("result", [])
        )

        if not signatures:
            return result

        result["available"] = True

        for tx in signatures:

            signature = tx.get(
                "signature"
            )

            if not signature:
                continue

            result["buyers"].append(
                signature
            )

        return result

    except Exception:
        return result


# ============================================================
# ALERT
# ============================================================

async def send_alert(context, candidate):

    if not TELEGRAM_BOT_TOKEN:
        return

    try:

        risk = candidate.get(
            "onchain_risk",
            {}
        )

        warnings = risk.get(
            "warnings",
            []
        )

        warning_text = ", ".join(
            warnings
        ) if warnings else "Keine"

        message = (
            "🚨 MEMECOIN ALERT\n\n"
            f"🪙 {candidate['name']} "
            f"({candidate['symbol']})\n"
            f"⛓️ Chain: {candidate['chain']}\n"
            f"⭐ Score: {candidate['score']}/100\n"
            f"💧 Liquidität: "
            f"${candidate['liquidity']:,.0f}\n"
            f"📊 Volumen 24h: "
            f"${candidate['volume']:,.0f}\n"
            f"🟢 Käufe: {candidate['buys']}\n"
            f"🔴 Verkäufe: {candidate['sells']}\n"
            f"📈 24h: {candidate['price_change']:.2f}%\n\n"
            f"🔐 On-Chain Risiko: "
            f"{format_risk(risk)}\n"
            f"⚠️ {warning_text}\n\n"
            f"🔗 {candidate['url']}"
        )

        await context.bot.send_message(
            chat_id=ALLOWED_CHAT_ID,
            text=message
        )

    except Exception:
        pass


# ============================================================
# SCAN
# ============================================================

async def perform_scan():

    checked = 0
    alerts = 0
    candidates = []

    async with aiohttp.ClientSession() as session:

        pairs = await get_latest_token_profiles(
            session
        )

        for pair in pairs:

            try:

                checked += 1

                chain = pair.get(
                    "chainId",
                    "unknown"
                )

                base = pair.get(
                    "baseToken"
                ) or {}

                symbol = base.get(
                    "symbol",
                    "UNKNOWN"
                )

                name = base.get(
                    "name",
                    symbol
                )

                address = base.get(
                    "address"
                )

                if not address:
                    continue

                liquidity = safe_float(
                    (pair.get("liquidity") or {}).get("usd")
                )

                volume = safe_float(
                    (pair.get("volume") or {}).get("h24")
                )

                txns = pair.get("txns") or {}
                h24 = txns.get("h24") or {}

                buys = safe_int(
                    h24.get("buys")
                )

                sells = safe_int(
                    h24.get("sells")
                )

                price_change = safe_float(
                    (pair.get("priceChange") or {}).get("h24")
                )

                score = calculate_score(
                    pair
                )

                market_risks = analyze_market_risk(
                    pair
                )

                onchain_risk = {
                    "risk": "N/A",
                    "warnings": []
                }

                if chain == "solana":

                    onchain_risk = await solana_risk_check(
                        session,
                        address
                    )

                    # Sicherheitsabzug
                    if onchain_risk["risk"] == "HIGH":
                        score -= 15

                    elif onchain_risk["risk"] == "MEDIUM":
                        score -= 5

                score = max(
                    0,
                    min(100, score)
                )

                url = pair.get(
                    "url",
                    f"https://dexscreener.com/{chain}/{pair.get('pairAddress', '')}"
                )

                candidate = {
                    "chain": chain,
                    "name": name,
                    "symbol": symbol,
                    "address": address,
                    "score": score,
                    "liquidity": liquidity,
                    "volume": volume,
                    "buys": buys,
                    "sells": sells,
                    "price_change": price_change,
                    "risks": market_risks,
                    "onchain_risk": onchain_risk,
                    "url": url,
                }

                candidates.append(
                    candidate
                )

            except Exception:
                continue

    # ========================================================
    # SORT
    # ========================================================

    candidates.sort(
        key=lambda x: (
            x["score"],
            x["liquidity"],
            x["volume"]
        ),
        reverse=True
    )

    # maximal 5 Kandidaten
    candidates = candidates[:5]

    # ========================================================
    # ALERT COUNT
    # ========================================================

    for candidate in candidates:

        if candidate["score"] >= ALERT_SCORE:

            alerts += 1

    return {
        "checked": checked,
        "alerts": alerts,
        "candidates": candidates
    }


# ============================================================
# TELEGRAM ACCESS
# ============================================================

def allowed(update):

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

    if not allowed(update):
        return

    await update.message.reply_text(
        "🤖 Memecoin Scanner aktiv.\n\n"
        f"Version: {APP_VERSION}\n\n"
        "/scan – Scanner starten\n"
        "/status – Status anzeigen\n"
        "/paper – Paper-Trading Status\n"
        "/buy – simulierten Trade eröffnen"
    )


# ============================================================
# /STATUS
# ============================================================

async def status_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not allowed(update):
        return

    await update.message.reply_text(
        "🟢 Scanner online\n\n"
        f"Version: {APP_VERSION}\n"
        f"Scan-Intervall: {SCAN_INTERVAL}s\n"
        f"Min. Liquidität: ${MIN_LIQUIDITY_USD:,.0f}\n"
        f"Min. 24h Volumen: ${MIN_VOLUME_24H:,.0f}\n"
        f"Alert Score: {ALERT_SCORE:.0f}"
    )


# ============================================================
# /SCAN
# ============================================================

async def scan_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not allowed(update):
        return

    await update.message.reply_text(
        "🔎 Scan läuft..."
    )

    result = await perform_scan()

    text = (
        "✅ Scan abgeschlossen.\n\n"
        f"🔎 Geprüft: {result['checked']}\n"
        f"🚨 Alerts: {result['alerts']}\n\n"
        "🏆 TOP-KANDIDATEN:\n"
    )

    if not result["candidates"]:

        text += (
            "\n❌ Keine passenden Kandidaten gefunden."
        )

    else:

        for index, candidate in enumerate(
            result["candidates"],
            start=1
        ):

            risk = candidate.get(
                "onchain_risk",
                {}
            )

            warnings = candidate.get(
                "risks",
                []
            )

            warning_text = ", ".join(
                warnings
            )

            text += (
                f"\n{index}. "
                f"{candidate['name']} "
                f"({candidate['symbol']})\n"
                f"   ⛓️ Chain: {candidate['chain']}\n"
                f"   ⭐ Score: "
                f"{candidate['score']:.0f}/100\n"
                f"   💧 Liquidität: "
                f"${candidate['liquidity']:,.2f}\n"
                f"   📊 Volumen 24h: "
                f"${candidate['volume']:,.2f}\n"
                f"   🟢 Käufe: "
                f"{candidate['buys']} | "
                f"🔴 Verkäufe: "
                f"{candidate['sells']}\n"
                f"   📈 24h: "
                f"{candidate['price_change']:.2f}%\n"
                f"   ⚠️ Risiken: "
                f"{warning_text}\n"
                f"   🔐 On-Chain Risiko: "
                f"{format_risk(risk)}\n"
            )

            if risk.get("warnings"):

                text += (
                    "   🛑 "
                    + ", ".join(
                        risk["warnings"]
                    )
                    + "\n"
                )

            text += (
                f"   🔗 {candidate['url']}\n"
            )

    await update.message.reply_text(
        text
    )


# ============================================================
# /PAPER
# ============================================================

async def paper_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not allowed(update):
        return

    conn = sqlite3.connect(
        DB_FILE
    )

    rows = conn.execute("""
        SELECT chain, symbol, side, price,
               amount_usd, created_at
        FROM paper_trades
        ORDER BY id DESC
        LIMIT 10
    """).fetchall()

    conn.close()

    if not rows:

        await update.message.reply_text(
            "📄 Noch keine Paper-Trades."
        )

        return

    text = "📄 PAPER TRADING\n\n"

    for row in rows:

        chain, symbol, side, price, amount, created = row

        text += (
            f"{side} {symbol}\n"
            f"⛓️ {chain}\n"
            f"💵 ${amount:.2f}\n"
            f"💰 Preis: {price}\n\n"
        )

    await update.message.reply_text(
        text
    )


# ============================================================
# /BUY
# ============================================================

async def buy_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not allowed(update):
        return

    await update.message.reply_text(
        "🧪 Paper-Trading ist aktiv.\n\n"
        "Es werden keine echten Käufe ausgeführt.\n"
        "Verwende zuerst /scan."
    )


# ============================================================
# BACKGROUND SCANNER
# ============================================================

async def scanner_loop(application):

    while True:

        try:

            result = await perform_scan()

            for candidate in result["candidates"]:

                if candidate["score"] >= ALERT_SCORE:

                    # Nur als Info:
                    # echter Trade ist weiterhin deaktiviert.
                    pass

        except Exception:
            pass

        await asyncio.sleep(
            SCAN_INTERVAL
        )


# ============================================================
# STARTUP
# ============================================================

async def post_init(application):

    init_db()

    asyncio.create_task(
        scanner_loop(application)
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
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
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
        f"Starting {APP_VERSION}"
    )

    application.run_polling()


if __name__ == "__main__":
    main()