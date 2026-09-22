import os
import sqlite3
import asyncio
from datetime import datetime, timezone

import aiohttp
import json
import time
import websockets
from aiohttp import web
from dotenv import load_dotenv

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

load_dotenv()


# ============================================================
# CONFIG
# ============================================================

APP_VERSION = "3.3.3-pro-diagnostics"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_CHAT_ID = os.getenv("ALLOWED_CHAT_ID", "")

SOLANA_RPC = os.getenv(
    "SOLANA_RPC",
    "https://api.mainnet-beta.solana.com"
)

ETH_RPC_URL = os.getenv(
    "ETH_RPC_URL",
    "https://ethereum-rpc.publicnode.com"
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

DB_FILE = os.getenv("DB_FILE", "scanner.db")
SOLANA_WS = os.getenv("SOLANA_WS", "wss://api.mainnet-beta.solana.com")
PORT = int(os.getenv("PORT", "10000"))
AUTO_ALERT = os.getenv("AUTO_ALERT", "true").lower() in {"1", "true", "yes", "on"}
PAPER_AMOUNT_USD = float(os.getenv("PAPER_AMOUNT_USD", "25"))
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "120"))

# Verified/default Solana launch/AMM programs. Extra IDs can be added via SOLANA_PROGRAM_IDS.
DEFAULT_SOLANA_PROGRAM_IDS = [
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",  # pump.fun
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",  # PumpSwap
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",  # Raydium CPMM
]
SOLANA_PROGRAM_IDS = [x.strip() for x in os.getenv(
    "SOLANA_PROGRAM_IDS", ",".join(DEFAULT_SOLANA_PROGRAM_IDS)
).split(",") if x.strip()]

scan_trigger = asyncio.Event()
last_scan_cache = []


# ============================================================
# EARLY FILTER
# ============================================================

MAX_LIQUIDITY_USD = 500000
MAX_VOLUME_24H = 500000

MIN_PAIR_AGE_MINUTES = 1
MAX_PAIR_AGE_HOURS = 72

MIN_TXNS_24H = 20

MAX_PRICE_CHANGE_24H = 500

EARLY_BUYER_WINDOW_HOURS = 24

SOLANA_SIGNATURE_LIMIT = 50

ETH_LOG_CHUNK_SIZE = 2000


# ============================================================
# TOKEN FILTER
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
# ERC20 TRANSFER EVENT
# ============================================================

ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


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

    conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            pair_address TEXT PRIMARY KEY,
            alerted_at REAL NOT NULL,
            score INTEGER NOT NULL
        )
    """)

    conn.commit()
    conn.close()


# ============================================================
# BASIC HELPERS
# ============================================================

def now():
    return datetime.now(timezone.utc)


def allowed(update: Update):

    if not ALLOWED_CHAT_ID:
        return True

    return str(update.effective_chat.id) == str(
        ALLOWED_CHAT_ID
    )


def clean_number(value, default=0):

    try:
        return float(value or 0)
    except Exception:
        return default


def format_usd(value):

    value = float(value or 0)

    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"

    if value >= 1_000:
        return f"${value / 1_000:.1f}K"

    return f"${value:.0f}"


def is_blocked_token(symbol, name=""):

    symbol_clean = (
        symbol or ""
    ).upper().strip()

    name_clean = (
        name or ""
    ).upper().strip()

    if symbol_clean in BLOCKED_SYMBOLS:
        return True

    for term in BLOCKED_NAME_TERMS:

        if term in symbol_clean:
            return True

        if term in name_clean:
            return True

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


def pair_age_hours(pair):

    created = pair.get(
        "pairCreatedAt"
    )

    if not created:
        return None

    try:

        created_seconds = (
            float(created) / 1000
        )

        created_dt = datetime.fromtimestamp(
            created_seconds,
            tz=timezone.utc
        )

        age = now() - created_dt

        return age.total_seconds() / 3600

    except Exception:
        return None


# ============================================================
# HTTP
# ============================================================

async def http_get_json(
    session,
    url,
    timeout=20
):

    try:

        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(
                total=timeout
            ),
            headers={
                "User-Agent":
                    "MemecoinScanner/3.3"
            },
        ) as response:

            if response.status != 200:
                return None

            return await response.json()

    except Exception:
        return None


async def rpc_call(
    session,
    rpc_url,
    method,
    params
):

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }

    try:

        async with session.post(
            rpc_url,
            json=payload,
            timeout=aiohttp.ClientTimeout(
                total=20
            ),
        ) as response:

            if response.status != 200:
                return None

            data = await response.json()

            if "error" in data:
                return None

            return data.get("result")

    except Exception:
        return None


# ============================================================
# DEXSCREENER
# ============================================================

async def get_latest_profiles(
    session
):

    url = (
        "https://api.dexscreener.com/"
        "token-profiles/latest/v1"
    )

    data = await http_get_json(
        session,
        url
    )

    if not data:
        return []

    if isinstance(data, list):
        return data

    return data.get(
        "tokens",
        []
    )


async def get_token_pairs(
    session,
    chain,
    address
):

    url = (
        "https://api.dexscreener.com/"
        f"token-pairs/v1/{chain}/{address}"
    )

    data = await http_get_json(
        session,
        url
    )

    if not data:
        return []

    if isinstance(data, list):
        return data

    return data.get(
        "pairs",
        []
    )


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
    "TOAD",
    "APE",
}


def meme_signal(
    name,
    symbol
):

    text = (
        f"{name or ''} "
        f"{symbol or ''}"
    ).upper()

    return any(
        term in text
        for term in MEME_TERMS
    )


# ============================================================
# DISCOVERY
# ============================================================

async def discover_candidates():

    candidates = []
    stats = {
        "profiles": 0, "pair_responses": 0, "pairs": 0,
        "solana_pairs": 0, "ethereum_pairs": 0, "unsupported_chain": 0,
        "missing_data": 0, "blocked": 0, "age_missing": 0,
        "too_new": 0, "too_old": 0, "liq_low": 0, "liq_high": 0,
        "vol_low": 0, "vol_high": 0, "txns_low": 0,
        "price_change_high": 0, "meme_filter": 0, "passed": 0,
        "pair_errors": 0,
    }

    async with aiohttp.ClientSession() as session:
        profiles = await get_latest_profiles(session)
        if not profiles:
            return [], stats

        profiles = profiles[:100]
        stats["profiles"] = len(profiles)
        tasks = []
        for profile in profiles:
            chain = profile.get("chainId")
            address = profile.get("tokenAddress")
            if not chain or not address:
                stats["missing_data"] += 1
                continue
            tasks.append(get_token_pairs(session, chain, address))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        stats["pair_responses"] = len(results)

        for pairs in results:
            if isinstance(pairs, Exception):
                stats["pair_errors"] += 1
                continue
            for pair in pairs:
                stats["pairs"] += 1
                try:
                    chain = (pair.get("chainId") or "").lower()
                    if chain == "solana": stats["solana_pairs"] += 1
                    elif chain == "ethereum": stats["ethereum_pairs"] += 1
                    else:
                        stats["unsupported_chain"] += 1
                        continue

                    pair_address = pair.get("pairAddress")
                    base = pair.get("baseToken") or {}
                    name = base.get("name", "Unknown")
                    symbol = base.get("symbol", "UNKNOWN")
                    token_address = base.get("address")
                    if not chain or not pair_address or not token_address:
                        stats["missing_data"] += 1; continue
                    if is_blocked_token(symbol, name):
                        stats["blocked"] += 1; continue

                    liquidity = clean_number((pair.get("liquidity") or {}).get("usd"))
                    volume = clean_number((pair.get("volume") or {}).get("h24"))
                    txns_24h = ((pair.get("txns") or {}).get("h24") or {})
                    buys = int(clean_number(txns_24h.get("buys")))
                    sells = int(clean_number(txns_24h.get("sells")))
                    total_txns = buys + sells
                    price_change = clean_number((pair.get("priceChange") or {}).get("h24"))
                    age_hours = pair_age_hours(pair)

                    if age_hours is None: stats["age_missing"] += 1; continue
                    if age_hours * 60 < MIN_PAIR_AGE_MINUTES: stats["too_new"] += 1; continue
                    if age_hours > MAX_PAIR_AGE_HOURS: stats["too_old"] += 1; continue
                    if liquidity < MIN_LIQUIDITY_USD: stats["liq_low"] += 1; continue
                    if liquidity > MAX_LIQUIDITY_USD: stats["liq_high"] += 1; continue
                    if volume < MIN_VOLUME_24H: stats["vol_low"] += 1; continue
                    if volume > MAX_VOLUME_24H: stats["vol_high"] += 1; continue
                    if total_txns < MIN_TXNS_24H: stats["txns_low"] += 1; continue
                    if price_change > MAX_PRICE_CHANGE_24H: stats["price_change_high"] += 1; continue
                    if not meme_signal(name, symbol): stats["meme_filter"] += 1; continue

                    candidates.append({
                        "chain": chain, "address": token_address, "pair_address": pair_address,
                        "name": name, "symbol": symbol, "liquidity": liquidity,
                        "volume": volume, "buys": buys, "sells": sells, "txns": total_txns,
                        "price_change": price_change, "age_hours": age_hours,
                        "price_usd": clean_number(pair.get("priceUsd")), "url": pair.get("url"),
                    })
                    stats["passed"] += 1
                except Exception:
                    stats["pair_errors"] += 1

    unique = {}
    for candidate in candidates:
        key = (candidate["chain"], candidate["pair_address"])
        if key not in unique: unique[key] = candidate
    stats["passed_unique"] = len(unique)
    return list(unique.values()), stats


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

    if (
        10_000
        <= liquidity
        <= 250_000
    ):
        score += 20

    elif liquidity >= 5_000:
        score += 12

    if (
        10_000
        <= volume
        <= 250_000
    ):
        score += 20

    elif volume >= 5_000:
        score += 12

    total = buys + sells

    if total > 0:

        ratio = (
            buys / total
        )

        if ratio >= 0.65:
            score += 20

        elif ratio >= 0.55:
            score += 12

        elif ratio >= 0.50:
            score += 6

    if age <= 6:
        score += 25

    elif age <= 24:
        score += 20

    elif age <= 48:
        score += 12

    elif age <= 72:
        score += 5

    change = c["price_change"]

    # Keine Belohnung mehr für extreme Pumps.
    if 0 <= change <= 50:
        score += 15

    elif 50 < change <= 150:
        score += 8

    elif -20 <= change < 0:
        score += 2

    elif -40 <= change < -20:
        score -= 8

    elif change < -40:
        score -= 25

    elif change > 300:
        score -= 15

    return max(
        0,
        min(score, 100)
    )


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
        risks.append(
            "Mehr Verkäufe als Käufe"
        )

    if liquidity < 10_000:
        risks.append(
            "Sehr geringe Liquidität"
        )

    if change > 150:
        risks.append(
            "Starker Preisanstieg"
        )

    if change <= -60:
        risks.append(
            "Extremer Preisabsturz"
        )
    elif change <= -35:
        risks.append(
            "Starker Preisrückgang"
        )

    if c["volume"] > (
        liquidity * 20
    ):
        risks.append(
            "Sehr hohes Volumen zur Liquidität"
        )

    if not risks:
        return "Keine offensichtlichen"

    return ", ".join(risks)


# ============================================================
# EVM HELPERS
# ============================================================

def topic_address(address):

    return (
        "0x"
        + "0" * 24
        + address.lower().replace(
            "0x",
            ""
        )
    )


def decode_topic_address(topic):

    if not topic:
        return None

    value = topic[-40:]

    return "0x" + value.lower()


def hex_to_int(value):

    if not value:
        return 0

    try:
        return int(
            value,
            16
        )
    except Exception:
        return 0


async def eth_get_block_number(
    session
):

    result = await rpc_call(
        session,
        ETH_RPC_URL,
        "eth_blockNumber",
        []
    )

    if not result:
        return None

    try:
        return int(
            result,
            16
        )
    except Exception:
        return None


async def eth_get_logs(
    session,
    address,
    from_block,
    to_block,
    topics
):

    params = [{
        "address": address,
        "fromBlock":
            hex(from_block),
        "toBlock":
            hex(to_block),
        "topics": topics,
    }]

    return await rpc_call(
        session,
        ETH_RPC_URL,
        "eth_getLogs",
        params
    )


async def eth_get_code(
    session,
    address
):

    return await rpc_call(
        session,
        ETH_RPC_URL,
        "eth_getCode",
        [
            address,
            "latest"
        ]
    )


# ============================================================
# ETHEREUM EARLY BUYERS
# ============================================================

async def ethereum_early_buyers(
    session,
    candidate
):

    result = {
        "buyers": [],
        "buyer_count": 0,
        "earliest_minutes": None,
        "status":
            "⚪ Keine Daten"
    }

    current_block = (
        await eth_get_block_number(
            session
        )
    )

    if current_block is None:
        result["status"] = (
            "⚪ RPC nicht verfügbar"
        )
        return result

    age_hours = candidate[
        "age_hours"
    ]

    # Sicherheitsmarge.
    blocks_back = int(
        (age_hours + 2)
        * 3600
        / 12
    )

    blocks_back = max(
        blocks_back,
        100
    )

    blocks_back = min(
        blocks_back,
        30_000
    )

    from_block = max(
        0,
        current_block - blocks_back
    )

    pair = candidate[
        "pair_address"
    ]

    token = candidate[
        "address"
    ]

    buyer_logs = []

    # Transfer-Events:
    # token -> pair = Verkauf
    # pair -> wallet = möglicher Kauf

    for start in range(
        from_block,
        current_block + 1,
        ETH_LOG_CHUNK_SIZE
    ):

        end = min(
            start
            + ETH_LOG_CHUNK_SIZE
            - 1,
            current_block
        )

        logs = await eth_get_logs(
            session,
            token,
            start,
            end,
            [
                ERC20_TRANSFER_TOPIC,
                topic_address(pair),
            ]
        )

        if not logs:
            continue

        buyer_logs.extend(
            logs
        )

        # RPC schonen.
        await asyncio.sleep(
            0.05
        )

    if not buyer_logs:
        result["status"] = (
            "⚪ Keine frühen Transfers"
        )
        return result

    wallets = {}

    # Nur Transfer "from pair -> wallet"
    for log in buyer_logs:

        topics = (
            log.get(
                "topics"
            )
            or []
        )

        if len(topics) < 3:
            continue

        to_address = (
            decode_topic_address(
                topics[2]
            )
        )

        if not to_address:
            continue

        if to_address.lower() == (
            pair.lower()
        ):
            continue

        # Contract-Adressen nicht als
        # normale Wallets werten.
        code = await eth_get_code(
            session,
            to_address
        )

        if code and code != "0x":
            continue

        block_number = hex_to_int(
            log.get(
                "blockNumber"
            )
        )

        timestamp = None

        # Der Block wird später zeitlich
        # über die Pair-Alter-Näherung
        # eingeordnet.
        wallets.setdefault(
            to_address,
            {
                "wallet":
                    to_address,
                "block":
                    block_number,
                "count":
                    1
            }
        )

        if block_number < wallets[
            to_address
        ]["block"]:
            wallets[
                to_address
            ]["block"
            ] = block_number

        else:
            wallets[
                to_address
            ]["count"] += 1

    if not wallets:
        result["status"] = (
            "⚪ Keine Wallet-Käufer erkannt"
        )
        return result

    ordered = sorted(
        wallets.values(),
        key=lambda x: x["block"]
    )

    result["buyers"] = ordered[:10]
    result["buyer_count"] = len(
        ordered
    )

    if age_hours <= 6:
        result["status"] = (
            "🟢 Frühe Wallet-Käufer erkannt"
        )

    else:
        result["status"] = (
            "🟡 Wallet-Käufer erkannt"
        )

    return result


# ============================================================
# SOLANA RPC
# ============================================================

async def solana_rpc_call(
    session,
    method,
    params
):

    return await rpc_call(
        session,
        SOLANA_RPC,
        method,
        params
    )


# ============================================================
# SOLANA EARLY BUYERS
# ============================================================

def solana_account_key(
    account
):

    if isinstance(
        account,
        str
    ):
        return account

    if isinstance(
        account,
        dict
    ):
        return account.get(
            "pubkey"
        )

    return None


def token_balance_map(
    balances,
    mint
):

    result = {}

    for item in (
        balances or []
    ):

        if item.get(
            "mint"
        ) != mint:
            continue

        owner = item.get(
            "owner"
        )

        if not owner:
            continue

        ui = (
            item.get(
                "uiTokenAmount"
            )
            or {}
        )

        amount = clean_number(
            ui.get(
                "uiAmount"
            )
        )

        result[owner] = (
            result.get(
                owner,
                0
            )
            + amount
        )

    return result


async def solana_early_buyers(session, candidate):
    result = {
        "buyers": [],
        "buyer_count": 0,
        "earliest_minutes": None,
        "status": "⚪ Keine Daten",
    }

    pair = candidate["pair_address"]
    mint = candidate["address"]

    # A Solana DEX swap is not guaranteed to be indexed under the pool/pair
    # address returned by Dexscreener. Query both pool and token mint, then
    # de-duplicate signatures before parsing token balance deltas.
    signature_infos = []
    seen_signatures = set()
    for address in (pair, mint):
        if not address:
            continue
        rows = await solana_rpc_call(
            session,
            "getSignaturesForAddress",
            [address, {"limit": SOLANA_SIGNATURE_LIMIT, "commitment": "confirmed"}],
        )
        for row in rows or []:
            sig = row.get("signature")
            if sig and sig not in seen_signatures:
                seen_signatures.add(sig)
                signature_infos.append(row)

    if not signature_infos:
        result["status"] = "⚪ Keine Token-/Pool-Transaktionen"
        return result

    # Process oldest first so the result represents the earliest buyers in
    # the sampled transaction window rather than whichever RPC row came first.
    signature_infos.sort(key=lambda x: x.get("blockTime") or 0)
    found = {}

    for signature_info in signature_infos:
        signature = signature_info.get("signature")
        if not signature:
            continue
        tx = await solana_rpc_call(
            session,
            "getTransaction",
            [signature, {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            }],
        )
        if not tx:
            continue
        meta = tx.get("meta") or {}
        if meta.get("err"):
            continue

        pre = token_balance_map(meta.get("preTokenBalances"), mint)
        post = token_balance_map(meta.get("postTokenBalances"), mint)
        block_time = tx.get("blockTime") or signature_info.get("blockTime")

        for owner in set(pre) | set(post):
            delta = post.get(owner, 0) - pre.get(owner, 0)
            if delta <= 0 or not owner:
                continue
            # Pool/program-owned balances normally have no useful wallet owner;
            # this also avoids the obvious pair address when it appears as owner.
            if owner.lower() == pair.lower():
                continue
            current = found.get(owner)
            if current is None:
                found[owner] = {
                    "wallet": owner,
                    "amount": delta,
                    "block_time": block_time,
                    "signature": signature,
                }
            else:
                current["amount"] += delta
                if block_time and (not current.get("block_time") or block_time < current["block_time"]):
                    current["block_time"] = block_time
                    current["signature"] = signature
        await asyncio.sleep(0.04)

    if not found:
        result["status"] = "⚪ Keine Käufer-Wallets im RPC-Fenster"
        return result

    ordered = sorted(found.values(), key=lambda x: x.get("block_time") or 0)
    result["buyers"] = ordered[:10]
    result["buyer_count"] = len(ordered)

    earliest = next((x.get("block_time") for x in ordered if x.get("block_time")), None)
    if earliest:
        result["earliest_minutes"] = max(0, int((time.time() - earliest) / 60))

    result["status"] = "🟢 Frühe Wallet-Käufer erkannt"
    return result


# ============================================================
# GENERIC EARLY BUYER ANALYSIS
# ============================================================

async def early_buyer_analysis(
    session,
    candidate
):

    chain = (
        candidate[
            "chain"
        ]
        or ""
    ).lower()

    if chain == "ethereum":

        return await ethereum_early_buyers(
            session,
            candidate
        )

    if chain == "solana":

        return await solana_early_buyers(
            session,
            candidate
        )

    return {
        "buyers": [],
        "buyer_count": 0,
        "earliest_minutes": None,
        "status":
            "⚪ Chain noch nicht unterstützt"
    }


# ============================================================
# SOLANA RISK
# ============================================================

async def solana_risk_check(
    session,
    mint
):

    result = await solana_rpc_call(
        session,
        "getAccountInfo",
        [
            mint,
            {
                "encoding":
                    "jsonParsed"
            }
        ]
    )

    if not result:
        return (
            "⚪ Nicht verfügbar"
        )

    try:

        value = result.get(
            "value"
        )

        if not value:
            return (
                "⚪ Nicht verfügbar"
            )

        parsed = (
            value
            .get(
                "data",
                {}
            )
            .get(
                "parsed",
                {}
            )
            .get(
                "info",
                {}
            )
        )

        mint_authority = (
            parsed.get(
                "mintAuthority"
            )
        )

        freeze_authority = (
            parsed.get(
                "freezeAuthority"
            )
        )

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
        return (
            "⚪ Nicht verfügbar"
        )


# ============================================================
# ANALYZE
# ============================================================

async def analyze_candidate(
    candidate
):

    candidate["score"] = (
        calculate_score(
            candidate
        )
    )

    candidate["market_risk"] = (
        analyze_market_risk(
            candidate
        )
    )

    async with aiohttp.ClientSession() as session:

        if candidate[
            "chain"
        ].lower() == "solana":

            candidate[
                "onchain_risk"
            ] = await solana_risk_check(
                session,
                candidate[
                    "address"
                ]
            )

        else:

            candidate[
                "onchain_risk"
            ] = (
                "⚪ Nicht verfügbar"
            )

        candidate[
            "early_buyers"
        ] = await early_buyer_analysis(
            session,
            candidate
        )

    # Bonus nur für tatsächlich
    # erkannte frühe Wallets.
    buyer_count = candidate[
        "early_buyers"
    ]["buyer_count"]

    if buyer_count >= 5:
        candidate["score"] += 8

    elif buyer_count >= 2:
        candidate["score"] += 5

    candidate["score"] = min(
        100,
        candidate["score"]
    )

    return candidate


# ============================================================
# SCAN
# ============================================================

async def perform_scan():

    global last_scan_cache
    raw, diagnostics = await discover_candidates()

    if not raw:

        return {
            "checked": 0,
            "analyzed": 0,
            "candidates": [],
            "diagnostics": diagnostics,
        }

    analyzed = []

    for candidate in raw:

        try:

            result = (
                await analyze_candidate(
                    candidate
                )
            )

            if result[
                "score"
            ] >= ALERT_SCORE:

                analyzed.append(
                    result
                )

        except Exception as e:

            print(
                "[ANALYZE ERROR]",
                type(e).__name__,
                str(e)
            )

    analyzed.sort(
        key=lambda x: (
            x["score"],
            x["early_buyers"][
                "buyer_count"
            ],
            -x["age_hours"]
        ),
        reverse=True
    )

    last_scan_cache = analyzed[:10]
    return {
        "checked": len(raw),
        "analyzed": len(raw),
        "candidates": analyzed[:5],
        "diagnostics": diagnostics,
    }


# ============================================================
# FORMAT EARLY BUYERS
# ============================================================

def format_early_buyers(
    data
):

    buyers = data.get(
        "buyers",
        []
    )

    status = data.get(
        "status",
        "⚪ Keine Daten"
    )

    count = data.get(
        "buyer_count",
        0
    )

    if not buyers:

        return (
            f"🐳 Early Buyers: {status}\n"
            f"   Erkannte Wallets: {count}"
        )

    text = (
        f"🐳 Early Buyers: {status}\n"
        f"   Erkannte Wallets: {count}\n"
    )

    for index, buyer in enumerate(
        buyers[:5],
        1
    ):

        wallet = buyer.get(
            "wallet",
            ""
        )

        short_wallet = (
            wallet[:6]
            + "..."
            + wallet[-4:]
        )

        amount = buyer.get(
            "amount"
        )

        if amount is not None:

            text += (
                f"   {index}. "
                f"{short_wallet}"
                f" | +{amount:.4f} Token\n"
            )

        else:

            text += (
                f"   {index}. "
                f"{short_wallet}\n"
            )

    return text.rstrip()


# ============================================================
# ALERT / PAPER HELPERS
# ============================================================

def alert_is_due(candidate):
    conn = sqlite3.connect(DB_FILE)
    row = conn.execute("SELECT alerted_at, score FROM alerts WHERE pair_address=?", (candidate["pair_address"],)).fetchone()
    conn.close()
    if not row:
        return True
    age_minutes = (time.time() - row[0]) / 60
    return age_minutes >= ALERT_COOLDOWN_MINUTES and candidate["score"] > row[1]


def mark_alerted(candidate):
    conn = sqlite3.connect(DB_FILE)
    conn.execute("INSERT OR REPLACE INTO alerts(pair_address, alerted_at, score) VALUES(?,?,?)", (candidate["pair_address"], time.time(), candidate["score"]))
    conn.commit(); conn.close()


def save_paper_trade(candidate, amount_usd=PAPER_AMOUNT_USD):
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""INSERT INTO paper_trades(created_at, token, symbol, chain, address, price, amount_usd, status)
                    VALUES(?,?,?,?,?,?,?,?)""", (now().isoformat(), candidate["name"], candidate["symbol"], candidate["chain"], candidate["address"], candidate["price_usd"], amount_usd, "OPEN"))
    conn.commit(); conn.close()


def candidate_keyboard(candidate):
    return InlineKeyboardMarkup([[InlineKeyboardButton(f"🧪 Paper Buy ${PAPER_AMOUNT_USD:.0f}", callback_data=f"paper:{candidate['pair_address']}")]])


# ============================================================
# FORMAT CANDIDATE
# ============================================================

def format_candidate(
    index,
    candidate
):

    age = candidate[
        "age_hours"
    ]

    if age < 1:

        age_text = (
            f"{age * 60:.0f} Min."
        )

    else:

        age_text = (
            f"{age:.1f} Std."
        )

    return (
        f"{index}. "
        f"{candidate['name']} "
        f"({candidate['symbol']})\n"
        f"   ⛓️ Chain: "
        f"{candidate['chain']}\n"
        f"   ⭐ Score: "
        f"{candidate['score']}/100\n"
        f"   🆕 Pair-Alter: "
        f"{age_text}\n"
        f"   💧 Liquidität: "
        f"{format_usd(candidate['liquidity'])}\n"
        f"   📊 Volumen 24h: "
        f"{format_usd(candidate['volume'])}\n"
        f"   🟢 Käufe: "
        f"{candidate['buys']} | "
        f"🔴 Verkäufe: "
        f"{candidate['sells']}\n"
        f"   📈 24h: "
        f"{candidate['price_change']:.2f}%\n"
        f"   👥 Aktivität: "
        f"{candidate['txns']} Txns\n"
        f"   {format_early_buyers(candidate)}\n"
        f"   ⚠️ Markt-Risiken: "
        f"{candidate['market_risk']}\n"
        f"   🔐 On-Chain Risiko: "
        f"{candidate['onchain_risk']}\n"
        f"   🔗 "
        f"{candidate['url'] or 'nicht verfügbar'}"
    )


# ============================================================
# TELEGRAM
# ============================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not allowed(update):
        return

    await update.message.reply_text(
        "🟢 Memecoin Scanner aktiv\n\n"
        f"Version: {APP_VERSION}\n"
        "Multichain: aktiv\n"
        "Early-Token-Filter: aktiv\n"
        "Real Early-Buyer Detection: aktiv\n"
        "Ethereum: aktiv\n"
        "Solana: aktiv\n"
        "Paper Trading: aktiv\n"
        "Risikoanalyse: aktiv\n\n"
        "Befehle:\n"
        "/status\n"
        "/scan\n"
        "/paper\n"
        "/buy"
    )


async def status(
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
        "Ethereum Analyse: aktiv\n"
        "Risikoanalyse: aktiv\n"
        "Early-Token-Filter: aktiv\n"
        "Real Early-Buyer Detection: aktiv\n"
        f"Solana WebSocket: {len(SOLANA_PROGRAM_IDS)} Programme\n"
        f"Auto-Alerts: {'aktiv' if AUTO_ALERT else 'aus'}\n"
        f"Health-Port: {PORT}\n\n"
        f"Version: {APP_VERSION}\n"
        f"Scan-Intervall: "
        f"{SCAN_INTERVAL}s\n"
        f"Min. Liquidität: "
        f"${MIN_LIQUIDITY_USD:,.0f}\n"
        f"Max. Liquidität: "
        f"${MAX_LIQUIDITY_USD:,.0f}\n"
        f"Min. Volumen: "
        f"${MIN_VOLUME_24H:,.0f}\n"
        f"Max. Volumen: "
        f"${MAX_VOLUME_24H:,.0f}\n"
        f"Max. Pair-Alter: "
        f"{MAX_PAIR_AGE_HOURS}h\n"
        f"Alert Score: "
        f"{ALERT_SCORE}"
    )


def format_diagnostics(stats):
    return (
        "\n\n🧪 Discovery-Diagnose:\n"
        f"Profile geladen: {stats.get('profiles', 0)}\n"
        f"Pairs gefunden: {stats.get('pairs', 0)} "
        f"(SOL {stats.get('solana_pairs', 0)} | ETH {stats.get('ethereum_pairs', 0)})\n"
        f"Andere Chains: {stats.get('unsupported_chain', 0)}\n"
        f"Fehlende Daten/Alter: {stats.get('missing_data', 0) + stats.get('age_missing', 0)}\n"
        f"Blockiert: {stats.get('blocked', 0)}\n"
        f"Zu neu: {stats.get('too_new', 0)} | Zu alt: {stats.get('too_old', 0)}\n"
        f"Liquidität zu niedrig/hoch: {stats.get('liq_low', 0)}/{stats.get('liq_high', 0)}\n"
        f"Volumen zu niedrig/hoch: {stats.get('vol_low', 0)}/{stats.get('vol_high', 0)}\n"
        f"Zu wenig Txns: {stats.get('txns_low', 0)}\n"
        f"Preisanstieg-Filter: {stats.get('price_change_high', 0)}\n"
        f"Meme-Filter: {stats.get('meme_filter', 0)}\n"
        f"Pair/API-Fehler: {stats.get('pair_errors', 0)}\n"
        f"Filter bestanden: {stats.get('passed_unique', stats.get('passed', 0))}"
    )


async def scan(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not allowed(update):
        return

    await update.message.reply_text(
        f"🔎 Starte {APP_VERSION} Early-Buyer-Analyse...\n\n"
        "Ethereum + Solana werden "
        "on-chain geprüft."
    )

    result = await perform_scan()

    checked = result[
        "checked"
    ]

    candidates = result[
        "candidates"
    ]

    diagnostics = result.get("diagnostics", {})
    diagnostic_text = format_diagnostics(diagnostics)

    if not candidates:

        await update.message.reply_text(
            "✅ Scan abgeschlossen.\n\n"
            f"🔎 Geprüft: {checked}\n"
            "🚨 Kandidaten: 0\n\n"
            "❌ Keine passenden "
            "Early-Kandidaten gefunden."
            + diagnostic_text
        )

        return

    text = (
        "✅ Scan abgeschlossen.\n\n"
        f"🔎 Geprüft: {checked}\n"
        f"🚨 Kandidaten: "
        f"{len(candidates)}\n\n"
        "🏆 TOP-EARLY-KANDIDATEN:\n\n"
    )

    for index, candidate in enumerate(
        candidates,
        1
    ):

        text += (
            format_candidate(
                index,
                candidate
            )
            + "\n\n"
        )

    text += diagnostic_text

    await update.message.reply_text(
        text,
        disable_web_page_preview=True
    )


async def paper(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not allowed(update):
        return

    conn = sqlite3.connect(
        DB_FILE
    )

    rows = conn.execute("""
        SELECT token, symbol, chain,
               price, amount_usd, status
        FROM paper_trades
        ORDER BY id DESC
        LIMIT 10
    """).fetchall()

    conn.close()

    if not rows:

        await update.message.reply_text(
            "💰 PAPER TRADING\n\n"
            "Noch keine Paper-Trades."
        )

        return

    text = (
        "💰 PAPER TRADING\n\n"
    )

    for row in rows:

        (
            token,
            symbol,
            chain,
            price,
            amount,
            status
        ) = row

        text += (
            f"{token} ({symbol})\n"
            f"⛓️ {chain}\n"
            f"💵 Entry: "
            f"${price:.8f}\n"
            f"💰 Betrag: "
            f"${amount:.2f}\n"
            f"📌 Status: "
            f"{status}\n\n"
        )

    await update.message.reply_text(
        text
    )


async def buy(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not allowed(update):
        return

    await update.message.reply_text(
        "🧪 PAPER BUY\n\n"
        "Paper Trading ist aktiv.\n"
        "Kein echtes Geld wird verwendet.\n\n"
        "Die echte Wallet-Erkennung "
        "läuft in Version 3.3."
    )


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    if not data.startswith("paper:"):
        return
    pair_address = data.split(":", 1)[1]
    candidate = next((c for c in last_scan_cache if c.get("pair_address") == pair_address), None)
    if not candidate:
        await query.message.reply_text("⚠️ Kandidat ist nicht mehr im aktuellen Scan-Cache. Bitte /scan erneut ausführen.")
        return
    save_paper_trade(candidate)
    await query.message.reply_text(
        f"🧪 Paper-Trade gespeichert\n{candidate['name']} ({candidate['symbol']})\n"
        f"Entry: ${candidate['price_usd']:.10f}\nBetrag: ${PAPER_AMOUNT_USD:.2f}\nKein echtes Geld wurde verwendet."
    )


async def health_handler(request):
    return web.json_response({"ok": True, "version": APP_VERSION, "scanner": "running"})


async def start_health_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[HEALTH] listening on 0.0.0.0:{PORT}")
    return runner


async def solana_ws_watcher():
    """Low-latency trigger. It does not trust logs as analysis; it wakes the verified Dex/RPC scan."""
    while True:
        try:
            async with websockets.connect(SOLANA_WS, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                for idx, program_id in enumerate(SOLANA_PROGRAM_IDS, 1):
                    await ws.send(json.dumps({
                        "jsonrpc": "2.0", "id": idx, "method": "logsSubscribe",
                        "params": [{"mentions": [program_id]}, {"commitment": "processed"}]
                    }))
                    await ws.recv()
                print(f"[WS] watching {len(SOLANA_PROGRAM_IDS)} Solana programs")
                async for message in ws:
                    payload = json.loads(message)
                    if payload.get("method") == "logsNotification":
                        scan_trigger.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print("[WS ERROR]", type(exc).__name__, str(exc))
            await asyncio.sleep(5)


# ============================================================
# BACKGROUND SCANNER
# ============================================================

async def scanner_loop(
    application
):

    while True:

        try:

            result = (
                await perform_scan()
            )

            candidates = result[
                "candidates"
            ]

            if candidates:
                print("[SCAN]", len(candidates), "Kandidaten gefunden.")
                for candidate in candidates:
                    buyers = candidate["early_buyers"]["buyer_count"]
                    print("[CANDIDATE]", candidate["name"], candidate["symbol"], candidate["chain"], "Score=", candidate["score"], "EarlyBuyers=", buyers)
                    if AUTO_ALERT and ALLOWED_CHAT_ID and alert_is_due(candidate):
                        try:
                            await application.bot.send_message(
                                chat_id=ALLOWED_CHAT_ID,
                                text="🚨 EARLY ALERT\n\n" + format_candidate(1, candidate),
                                disable_web_page_preview=True,
                                reply_markup=candidate_keyboard(candidate),
                            )
                            mark_alerted(candidate)
                        except Exception as exc:
                            print("[ALERT ERROR]", type(exc).__name__, str(exc))

        except Exception as e:

            print(
                "[SCANNER ERROR]",
                type(e).__name__,
                str(e)
            )

        try:
            await asyncio.wait_for(scan_trigger.wait(), timeout=SCAN_INTERVAL)
            scan_trigger.clear()
            await asyncio.sleep(2)  # give indexers a moment to expose the new pair
        except asyncio.TimeoutError:
            pass


async def post_init(
    application
):

    init_db()

    application.create_task(scanner_loop(application))
    application.create_task(solana_ws_watcher())
    application.create_task(start_health_server())


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
        .token(
            TELEGRAM_BOT_TOKEN
        )
        .post_init(
            post_init
        )
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start
        )
    )

    application.add_handler(
        CommandHandler(
            "status",
            status
        )
    )

    application.add_handler(
        CommandHandler(
            "scan",
            scan
        )
    )

    application.add_handler(
        CommandHandler(
            "paper",
            paper
        )
    )

    application.add_handler(
        CommandHandler(
            "buy",
            buy
        )
    )

    application.add_handler(CallbackQueryHandler(button_callback))

    print(
        f"🚀 Memecoin Scanner "
        f"{APP_VERSION} gestartet"
    )

    application.run_polling()


if __name__ == "__main__":
    main()