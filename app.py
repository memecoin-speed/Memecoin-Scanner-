import os
import sqlite3
import asyncio
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

load_dotenv()


# ============================================================
# CONFIG
# ============================================================

APP_VERSION = "3.2-early-filter"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_CHAT_ID = os.getenv("ALLOWED_CHAT_ID", "")

SOLANA_RPC = os.getenv(
    "SOLANA_RPC",
    "https://api.mainnet-beta.solana.com"
)

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "30"))

MIN_LIQUIDITY_USD = float(
    os.getenv("MIN_LIQUIDITY_USD", "5000")
)

MIN_VOLUME_24H = float(
    os.getenv("MIN_VOLUME_24H", "5000")
)

ALERT_SCORE = int(
    os.getenv("ALERT_SCORE", "65")
)

DB_FILE = os.getenv("DB_FILE", "scanner.db")


# ============================================================
# EARLY TOKEN FILTER
# ============================================================

MAX_LIQUIDITY_USD = 500000
MAX_VOLUME_24H = 500000

MIN_PAIR_AGE_MINUTES = 1
MAX_PAIR_AGE_HOURS = 72

MIN_TXNS_24H = 20

MAX_PRICE_CHANGE_24H = 500


# ============================================================
# BASE TOKENS / STABLECOINS
# ============================================================

BLOCKED_SYMBOLS = {
    "SOL",
    "WSOL",
    "ETH",
    "WETH",
    "BTC",
    "WBTC",
    "BNB",
    "WBNB",
    "MATIC",
    "WMATIC",
    "AVAX",
    "WAVAX",
    "FTM",
    "WFTM",

    "USDC",
    "USDT",
    "DAI",
    "USDE",
    "FDUSD",
    "TUSD",
    "USDD",

    "JITOSOL",
    "MSOL",
    "BSOL",
    "STSOL",
    "JUPSOL",
    "INF",
}


BLOCKED_NAME_TERMS = {
    "WRAPPED",
    "STAKED SOL",
    "LIQUID STAKING",
    "LST",
}


# ============================================================
# DATABASE
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_FILE)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            token TEXT,
            symbol TEXT,
            chain TEXT,
            address TEXT,
            score INTEGER,
            liquidity REAL,
            volume REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            token TEXT,
            symbol TEXT,
            chain TEXT,
            address TEXT,
            price REAL,
            amount_usd REAL,
            status TEXT
        )
    """)

    conn.commit()
    conn.close()


# ============================================================
# HELPERS
# ============================================================

def now():
    return datetime.now(timezone.utc)


def allowed(update: Update):
    if not ALLOWED_CHAT_ID:
        return True

    return str(update.effective_chat.id) == str(ALLOWED_CHAT_ID)


def clean_number(value, default=0):
    try:
        return float(value or 0)
    except Exception:
        return default


def is_blocked_token(symbol, name=""):
    symbol_clean = (symbol or "").upper().strip()
    name_clean = (name or "").upper().strip()

    if symbol_clean in BLOCKED_SYMBOLS:
        return True

    for term in BLOCKED_NAME_TERMS:
        if term in symbol_clean or term in name_clean:
            return True

    # Nur offensichtliche Wrapped-Tokens blockieren.
    if symbol_clean.startswith("W") and len(symbol_clean) <= 6:
        if symbol_clean in {
            "WETH",
            "WBTC",
            "WBNB",
            "WAVAX",
            "WFTM",
            "WSOL",
            "WMATIC",
        }:
            return True

    return False


def format_usd(value):
    value = float(value or 0)

    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"

    if value >= 1_000:
        return f"${value / 1_000:.1f}K"

    return f"${value:.0f}"


def pair_age_hours(pair):
    created = pair.get("pairCreatedAt")

    if not created:
        return None

    try:
        created_seconds = float(created) / 1000
        created_dt = datetime.fromtimestamp(
            created_seconds,
            tz=timezone.utc
        )

        age = now() - created_dt

        return age.total_seconds() / 3600

    except Exception:
        return None


# ============================================================
# DEXSCREENER
# ============================================================

async def http_get_json(session, url, timeout=15):
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={
                "User-Agent": "MemecoinScanner/3.2"
            },
        ) as response:

            if response.status != 200:
                return None

            return await response.json()

    except Exception:
        return None


async def get_latest_profiles(session):
    url = "https://api.dexscreener.com/token-profiles/latest/v1"

    data = await http_get_json(session, url)

    if not data:
        return []

    if isinstance(data, list):
        return data

    return data.get("tokens", [])


async def get_token_pairs(session, chain, address):
    url = (
        f"https://api.dexscreener.com"
        f"/token-pairs/v1/{chain}/{address}"
    )

    data = await http_get_json(session, url)

    if not data:
        return []

    if isinstance(data, list):
        return data

    return data.get("pairs", [])


# ============================================================
# MEME SIGNAL
# ============================================================

MEME_TERMS = {
    "PEPE",
    "DOGE",
    "DOG",
    "CAT",
    "FROG",
    "SHIB",
    "INU",
    "WOJAK",
    "MEME",
    "PUMP",
    "MOON",
    "AI",
    "TRUMP",
    "ELON",
    "BONK",
    "BRETT",
    "MOG",
    "CHAD",
    "COIN",
    "TOAD",
    "APE",
}


def meme_signal(name, symbol):
    text = (
        f"{name or ''} "
        f"{symbol or ''}"
    ).upper()

    for term in MEME_TERMS:
        if term in text:
            return True

    return False


# ============================================================
# CANDIDATE DISCOVERY
# ============================================================

async def discover_candidates():

    candidates = []

    async with aiohttp.ClientSession() as session:

        profiles = await get_latest_profiles(session)

        if not profiles:
            return []

        # Maximal 100 Profile-Einträge verarbeiten.
        profiles = profiles[:100]

        tasks = []

        for profile in profiles:

            chain = profile.get("chainId")
            address = profile.get("tokenAddress")

            if not chain or not address:
                continue

            tasks.append(
                get_token_pairs(
                    session,
                    chain,
                    address
                )
            )

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

        for pairs in results:

            if isinstance(pairs, Exception):
                continue

            for pair in pairs:

                try:
                    chain = pair.get("chainId")
                    address = pair.get("pairAddress")

                    base = pair.get("baseToken") or {}

                    name = base.get("name", "Unknown")
                    symbol = base.get("symbol", "UNKNOWN")

                    if not chain or not address:
                        continue

                    # Basis-Token filtern
                    if is_blocked_token(symbol, name):
                        continue

                    liquidity = clean_number(
                        (pair.get("liquidity") or {}).get("usd")
                    )

                    volume = clean_number(
                        (pair.get("volume") or {}).get("h24")
                    )

                    txns = pair.get("txns") or {}
                    txns_24h = txns.get("h24") or {}

                    buys = int(
                        clean_number(txns_24h.get("buys"))
                    )

                    sells = int(
                        clean_number(txns_24h.get("sells"))
                    )

                    total_txns = buys + sells

                    price_change = clean_number(
                        (pair.get("priceChange") or {}).get("h24")
                    )

                    age_hours = pair_age_hours(pair)

                    # Alter muss bekannt sein
                    if age_hours is None:
                        continue

                    age_minutes = age_hours * 60

                    if age_minutes < MIN_PAIR_AGE_MINUTES:
                        continue

                    if age_hours > MAX_PAIR_AGE_HOURS:
                        continue

                    # Liquidität
                    if liquidity < MIN_LIQUIDITY_USD:
                        continue

                    if liquidity > MAX_LIQUIDITY_USD:
                        continue

                    # Volumen
                    if volume < MIN_VOLUME_24H:
                        continue

                    if volume > MAX_VOLUME_24H:
                        continue

                    # Aktivität
                    if total_txns < MIN_TXNS_24H:
                        continue

                    # Extrem-Pumps vermeiden
                    if price_change > MAX_PRICE_CHANGE_24H:
                        continue

                    # Meme-Signal
                    meme = meme_signal(
                        name,
                        symbol
                    )

                    if not meme:
                        continue

                    candidate = {
                        "chain": chain,
                        "address": address,
                        "pair_address": address,
                        "name": name,
                        "symbol": symbol,
                        "liquidity": liquidity,
                        "volume": volume,
                        "buys": buys,
                        "sells": sells,
                        "txns": total_txns,
                        "price_change": price_change,
                        "age_hours": age_hours,
                        "price_usd": clean_number(
                            pair.get("priceUsd")
                        ),
                        "url": pair.get("url"),
                        "pair": pair,
                    }

                    candidates.append(candidate)

                except Exception:
                    continue

    # Doppelte Paare entfernen
    unique = {}

    for candidate in candidates:

        key = (
            candidate["chain"],
            candidate["pair_address"]
        )

        old = unique.get(key)

        if old is None:
            unique[key] = candidate
        else:
            if candidate["volume"] > old["volume"]:
                unique[key] = candidate

    return list(unique.values())


# ============================================================
# SCORING
# ============================================================

def calculate_score(c):

    score = 0

    liquidity = c["liquidity"]
    volume = c["volume"]
    buys = c["buys"]
    sells = c["sells"]
    age = c["age_hours"]

    total = buys + sells

    # Liquidität
    if 10_000 <= liquidity <= 250_000:
        score += 20
    elif liquidity >= 5_000:
        score += 12

    # Volumen
    if 10_000 <= volume <= 250_000:
        score += 20
    elif volume >= 5_000:
        score += 12

    # Kaufdruck
    if total > 0:
        buy_ratio = buys / total

        if buy_ratio >= 0.65:
            score += 20
        elif buy_ratio >= 0.55:
            score += 12
        elif buy_ratio >= 0.50:
            score += 6

    # Alter
    if age <= 6:
        score += 25
    elif age <= 24:
        score += 20
    elif age <= 48:
        score += 12
    elif age <= 72:
        score += 5

    # Preisbewegung
    change = c["price_change"]

    if 0 <= change <= 50:
        score += 15
    elif 50 < change <= 150:
        score += 8
    elif change < 0:
        score += 4

    return min(score, 100)


# ============================================================
# MARKET RISK
# ============================================================

def analyze_market_risk(c):

    risks = []

    buys = c["buys"]
    sells = c["sells"]
    change = c["price_change"]
    liquidity = c["liquidity"]

    if sells > buys * 1.25:
        risks.append("Mehr Verkäufe als Käufe")

    if liquidity < 10_000:
        risks.append("Sehr geringe Liquidität")

    if change > 150:
        risks.append("Starker Preisanstieg")

    if c["volume"] > liquidity * 20:
        risks.append("Sehr hohes Volumen zur Liquidität")

    if not risks:
        return "Keine offensichtlichen"

    return ", ".join(risks)


# ============================================================
# SOLANA ON-CHAIN RISK
# ============================================================

async def solana_rpc_call(session, method, params):

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
            timeout=aiohttp.ClientTimeout(total=15),
        ) as response:

            if response.status != 200:
                return None

            data = await response.json()

            return data.get("result")

    except Exception:
        return None


async def solana_risk_check(session, mint):

    result = await solana_rpc_call(
        session,
        "getAccountInfo",
        [
            mint,
            {
                "encoding": "jsonParsed"
            }
        ]
    )

    if not result:
        return "⚪ Nicht verfügbar"

    try:

        value = result.get("value")

        if not value:
            return "⚪ Nicht verfügbar"

        parsed = (
            value
            .get("data", {})
            .get("parsed", {})
            .get("info", {})
        )

        mint_authority = parsed.get("mintAuthority")
        freeze_authority = parsed.get("freezeAuthority")

        warnings = 0

        if mint_authority:
            warnings += 1

        if freeze_authority:
            warnings += 1

        if warnings == 0:
            return "🟢 Niedrig"

        if warnings == 1:
            return "🟡 Mittel"

        return "🔴 Hoch"

    except Exception:
        return "⚪ Nicht verfügbar"


# ============================================================
# EARLY BUYER SNAPSHOT
# ============================================================

async def early_buyer_snapshot(session, candidate):

    # Diese Version liefert bewusst nur eine Aktivitätsbewertung.
    # Die echte Wallet-Erkennung kommt in der nächsten Stufe.

    buys = candidate["buys"]
    sells = candidate["sells"]

    if buys + sells == 0:
        return "Keine Daten"

    ratio = buys / (buys + sells)

    if ratio >= 0.70:
        return "🟢 Starker Kaufdruck"

    if ratio >= 0.55:
        return "🟡 Kaufdruck"

    return "🔴 Kein klarer Kaufdruck"


# ============================================================
# ANALYSE
# ============================================================

async def analyze_candidate(candidate):

    candidate["score"] = calculate_score(
        candidate
    )

    candidate["market_risk"] = analyze_market_risk(
        candidate
    )

    async with aiohttp.ClientSession() as session:

        if candidate["chain"] == "solana":
            candidate["onchain_risk"] = (
                await solana_risk_check(
                    session,
                    candidate["address"]
                )
            )
        else:
            candidate["onchain_risk"] = (
                "⚪ Nicht verfügbar"
            )

        candidate["early_buyers"] = (
            await early_buyer_snapshot(
                session,
                candidate
            )
        )

    return candidate


# ============================================================
# SCAN
# ============================================================

async def perform_scan():

    raw = await discover_candidates()

    if not raw:
        return {
            "checked": 0,
            "candidates": []
        }

    analyzed = []

    for candidate in raw:

        try:

            result = await analyze_candidate(
                candidate
            )

            if result["score"] >= ALERT_SCORE:
                analyzed.append(result)

        except Exception:
            continue

    analyzed.sort(
        key=lambda x: (
            x["score"],
            -x["age_hours"],
            x["volume"]
        ),
        reverse=True
    )

    return {
        "checked": len(raw),
        "candidates": analyzed[:5]
    }


# ============================================================
# FORMAT SCAN
# ============================================================

def format_candidate(index, c):

    age = c["age_hours"]

    if age < 1:
        age_text = f"{age * 60:.0f} Min."
    else:
        age_text = f"{age:.1f} Std."

    return (
        f"{index}. {c['name']} ({c['symbol']})\n"
        f"   ⛓️ Chain: {c['chain']}\n"
        f"   ⭐ Score: {c['score']}/100\n"
        f"   🆕 Pair-Alter: {age_text}\n"
        f"   💧 Liquidität: {format_usd(c['liquidity'])}\n"
        f"   📊 Volumen 24h: {format_usd(c['volume'])}\n"
        f"   🟢 Käufe: {c['buys']} | "
        f"🔴 Verkäufe: {c['sells']}\n"
        f"   📈 24h: {c['price_change']:.2f}%\n"
        f"   👥 Aktivität: {c['txns']} Txns\n"
        f"   🐳 Käufer-Signal: {c['early_buyers']}\n"
        f"   ⚠️ Markt-Risiken: {c['market_risk']}\n"
        f"   🔐 On-Chain Risiko: {c['onchain_risk']}\n"
        f"   🔗 {c['url'] or 'nicht verfügbar'}"
    )


# ============================================================
# TELEGRAM
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not allowed(update):
        return

    await update.message.reply_text(
        "🟢 Memecoin Scanner aktiv\n\n"
        f"Version: {APP_VERSION}\n"
        "Multichain: aktiv\n"
        "Early-Token-Filter: aktiv\n"
        "Paper Trading: aktiv\n"
        "Solana Analyse: aktiv\n"
        "Risikoanalyse: aktiv\n\n"
        "Befehle:\n"
        "/status\n"
        "/scan\n"
        "/paper\n"
        "/buy"
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not allowed(update):
        return

    await update.message.reply_text(
        "🟢 BOT STATUS\n\n"
        "Scanner: aktiv\n"
        "DexScreener: aktiv\n"
        "Paper Trading: aktiv\n"
        "Solana Analyse: aktiv\n"
        "Risikoanalyse: aktiv\n"
        "Early-Token-Filter: aktiv\n\n"
        f"Version: {APP_VERSION}\n"
        f"Scan-Intervall: {SCAN_INTERVAL}s\n"
        f"Min. Liquidität: ${MIN_LIQUIDITY_USD:,.0f}\n"
        f"Max. Liquidität: ${MAX_LIQUIDITY_USD:,.0f}\n"
        f"Min. Volumen: ${MIN_VOLUME_24H:,.0f}\n"
        f"Max. Volumen: ${MAX_VOLUME_24H:,.0f}\n"
        f"Max. Pair-Alter: {MAX_PAIR_AGE_HOURS}h\n"
        f"Alert Score: {ALERT_SCORE}"
    )


async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not allowed(update):
        return

    await update.message.reply_text(
        "🔎 Starte Multichain Early-Token-Scan..."
    )

    result = await perform_scan()

    checked = result["checked"]
    candidates = result["candidates"]

    if not candidates:

        await update.message.reply_text(
            "✅ Scan abgeschlossen.\n\n"
            f"🔎 Geprüft: {checked}\n"
            "🚨 Kandidaten: 0\n\n"
            "❌ Keine passenden Early-Kandidaten gefunden.\n\n"
            "Filter:\n"
            f"• Pair-Alter: maximal {MAX_PAIR_AGE_HOURS}h\n"
            f"• Liquidität: ${MIN_LIQUIDITY_USD:,.0f}"
            f"–${MAX_LIQUIDITY_USD:,.0f}\n"
            f"• Volumen: ${MIN_VOLUME_24H:,.0f}"
            f"–${MAX_VOLUME_24H:,.0f}\n"
            f"• Mindestens {MIN_TXNS_24H} Transaktionen"
        )

        return

    text = (
        "✅ Scan abgeschlossen.\n\n"
        f"🔎 Geprüft: {checked}\n"
        f"🚨 Kandidaten: {len(candidates)}\n\n"
        "🏆 TOP-EARLY-KANDIDATEN:\n\n"
    )

    for i, candidate in enumerate(candidates, 1):

        text += (
            format_candidate(
                i,
                candidate
            )
            + "\n\n"
        )

    await update.message.reply_text(
        text,
        disable_web_page_preview=True
    )


async def paper(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not allowed(update):
        return

    conn = sqlite3.connect(DB_FILE)

    rows = conn.execute("""
        SELECT token, symbol, chain, price,
               amount_usd, status
        FROM paper_trades
        ORDER BY id DESC
        LIMIT 10
    """).fetchall()

    conn.close()

    if not rows:

        await update.message.reply_text(
            "💰 PAPER TRADING\n\n"
            "Noch keine Paper-Trades vorhanden."
        )

        return

    text = "💰 PAPER TRADING\n\n"

    for row in rows:

        token, symbol, chain, price, amount, status = row

        text += (
            f"{token} ({symbol})\n"
            f"⛓️ {chain}\n"
            f"💵 Entry: ${price:.8f}\n"
            f"💰 Betrag: ${amount:.2f}\n"
            f"📌 Status: {status}\n\n"
        )

    await update.message.reply_text(text)


async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not allowed(update):
        return

    await update.message.reply_text(
        "🧪 PAPER BUY\n\n"
        "Paper Trading ist aktiv.\n"
        "Kein echtes Geld wird verwendet.\n\n"
        "Für einen Paper-Buy benötige ich künftig "
        "den Token aus einem Scanner-Treffer."
    )


# ============================================================
# BACKGROUND SCANNER
# ============================================================

async def scanner_loop(application):

    while True:

        try:

            result = await perform_scan()

            candidates = result["candidates"]

            if candidates:

                print(
                    f"[SCAN] "
                    f"{len(candidates)} Kandidaten gefunden."
                )

                for candidate in candidates:

                    print(
                        f"[CANDIDATE] "
                        f"{candidate['name']} "
                        f"({candidate['symbol']}) "
                        f"{candidate['chain']} "
                        f"Score={candidate['score']}"
                    )

        except Exception as e:

            print(
                f"[SCANNER ERROR] {type(e).__name__}: {e}"
            )

        await asyncio.sleep(SCAN_INTERVAL)


async def post_init(application):

    init_db()

    application.create_task(
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
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    application.add_handler(
        CommandHandler("start", start)
    )

    application.add_handler(
        CommandHandler("status", status)
    )

    application.add_handler(
        CommandHandler("scan", scan)
    )

    application.add_handler(
        CommandHandler("paper", paper)
    )

    application.add_handler(
        CommandHandler("buy", buy)
    )

    print(
        f"🚀 Memecoin Scanner {APP_VERSION} gestartet"
    )

    application.run_polling()


if __name__ == "__main__":
    main()