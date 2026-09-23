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

APP_VERSION = "3.5.8-pro-token-dedupe-precheck-selection"

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
scan_lock = asyncio.Lock()
manual_scan_tasks = set()

# Resilience caches: a temporary HTTP 429 must not erase otherwise usable discovery.
DISCOVERY_CACHE_TTL = int(os.getenv("DISCOVERY_CACHE_TTL", "3600"))
PERSISTENT_PAIR_CACHE_TTL = int(os.getenv("PERSISTENT_PAIR_CACHE_TTL", "21600"))
PAIR_CACHE_TTL = int(os.getenv("PAIR_CACHE_TTL", "900"))
discovery_feed_cache = {}
token_pair_cache = {}
# Keep public RPC pressure deliberately low. Buyer analyses can run concurrently,
# but the RPC transport itself is throttled to avoid burst 429s.
solana_rpc_semaphore = asyncio.Semaphore(int(os.getenv("SOLANA_RPC_CONCURRENCY", "2")))
SOLANA_TX_CACHE_TTL = int(os.getenv("SOLANA_TX_CACHE_TTL", "900"))
SOLANA_RPC_429_COOLDOWN = float(os.getenv("SOLANA_RPC_429_COOLDOWN", "1.5"))
solana_tx_cache = {}          # signature -> (timestamp, tx, source)
solana_tx_inflight = {}       # signature -> Future/Task, coalesces duplicate reads
solana_rpc_cooldown_until = {}


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

SOLANA_SIGNATURE_LIMIT = int(os.getenv("SOLANA_SIGNATURE_LIMIT", "8"))
RPC_CALL_TIMEOUT = int(os.getenv("RPC_CALL_TIMEOUT", "4"))
CANDIDATE_ANALYSIS_TIMEOUT = int(os.getenv("CANDIDATE_ANALYSIS_TIMEOUT", "24"))
DISCOVERY_STAGE_TIMEOUT = int(os.getenv("DISCOVERY_STAGE_TIMEOUT", "40"))
SOLANA_DIRECT_LIMIT = int(os.getenv("SOLANA_DIRECT_LIMIT", "12"))
SOLANA_DIRECT_SIGS_PER_PROGRAM = int(os.getenv("SOLANA_DIRECT_SIGS_PER_PROGRAM", "6"))
SOLANA_DIRECT_TX_LIMIT = int(os.getenv("SOLANA_DIRECT_TX_LIMIT", "10"))
SOLANA_DIRECT_TTL_SECONDS = int(os.getenv("SOLANA_DIRECT_TTL_SECONDS", "900"))
solana_direct_signatures = {}
SOLANA_RPC_FALLBACKS = [x.strip() for x in os.getenv(
    "SOLANA_RPC_FALLBACKS", "https://solana-rpc.publicnode.com"
).split(",") if x.strip()]

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

    conn.execute("""
        CREATE TABLE IF NOT EXISTS discovery_pair_cache (
            cache_key TEXT PRIMARY KEY,
            saved_at REAL NOT NULL,
            pair_json TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()



def save_persistent_pairs(pairs):
    """Persist successful discovery pairs so a temporary 429/timeout cannot zero the next scan."""
    if not pairs:
        return
    conn = sqlite3.connect(DB_FILE)
    now_ts = time.time()
    try:
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            key = f"{(pair.get('chainId') or '').lower()}:{pair.get('pairAddress') or ''}"
            if key == ":":
                continue
            conn.execute(
                "INSERT OR REPLACE INTO discovery_pair_cache(cache_key,saved_at,pair_json) VALUES(?,?,?)",
                (key, now_ts, json.dumps(pair, separators=(",", ":")))
            )
        conn.execute("DELETE FROM discovery_pair_cache WHERE saved_at < ?", (now_ts - PERSISTENT_PAIR_CACHE_TTL,))
        conn.commit()
    finally:
        conn.close()

def load_persistent_pairs(limit=250):
    conn = sqlite3.connect(DB_FILE)
    try:
        rows = conn.execute(
            "SELECT pair_json FROM discovery_pair_cache WHERE saved_at >= ? ORDER BY saved_at DESC LIMIT ?",
            (time.time() - PERSISTENT_PAIR_CACHE_TTL, limit)
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()
    out=[]
    for (raw,) in rows:
        try:
            item=json.loads(raw)
            if isinstance(item,dict): out.append(item)
        except Exception:
            pass
    return out

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

async def get_discovery_feed(session, path):
    """Fetch DexScreener with retry/backoff and stale-cache fallback on rate limits."""
    url = f"https://api.dexscreener.com/{path}"
    error = None
    for attempt in range(3):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20),
                headers={"User-Agent": f"MemecoinScanner/{APP_VERSION}", "Accept": "application/json"}) as response:
                if response.status == 200:
                    data = await response.json(content_type=None)
                    if isinstance(data, list): items = data
                    elif isinstance(data, dict): items = data.get("tokens", []) or data.get("pairs", []) or []
                    else: items = []
                    discovery_feed_cache[path] = (time.time(), items)
                    return items, None
                error = f"HTTP {response.status}"
                if response.status != 429: break
        except Exception as exc:
            error = f"{type(exc).__name__}: {str(exc)[:80]}"
        await asyncio.sleep(1.5 * (attempt + 1))
    cached = discovery_feed_cache.get(path)
    if cached and time.time() - cached[0] <= DISCOVERY_CACHE_TTL:
        return cached[1], f"{error or 'error'} -> cache:{len(cached[1])}"
    return [], error or "request failed"


async def get_latest_profiles(session):
    items, _ = await get_discovery_feed(session, "token-profiles/latest/v1")
    return items


async def get_discovery_profiles(session):
    # DexScreener is enrichment only. Calls are sequential to avoid burst 429s.
    feeds = [
        ("profiles", "token-profiles/latest/v1"),
        ("boosts_latest", "token-boosts/latest/v1"),
        ("boosts_top", "token-boosts/top/v1"),
    ]
    merged, seen, health = [], set(), {}
    for name, path in feeds:
        items, error = await get_discovery_feed(session, path)
        health[name] = error or f"ok:{len(items)}"
        for item in items:
            chain = (item.get("chainId") or "").lower()
            address = item.get("tokenAddress")
            if chain not in {"solana", "ethereum"} or not address:
                continue
            key = (chain, address.lower())
            if key not in seen:
                seen.add(key); merged.append(item)
        # Be polite to the public endpoint and avoid three simultaneous requests.
        await asyncio.sleep(1.0)
    return merged, health


def _gecko_token_id_to_address(token_id):
    if not token_id: return None
    return token_id.split("_", 1)[1] if "_" in token_id else token_id


async def get_gecko_new_pools(session, network, chain):
    """Independent discovery fallback. Returns DexScreener-shaped pairs."""
    url = f"https://api.geckoterminal.com/api/v2/networks/{network}/new_pools?page=1&include=base_token"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20), headers={"Accept":"application/json","User-Agent":f"MemecoinScanner/{APP_VERSION}"}) as r:
            if r.status != 200:
                return [], f"HTTP {r.status}"
            payload = await r.json(content_type=None)
    except Exception as exc:
        return [], f"{type(exc).__name__}: {str(exc)[:70]}"
    included = {}
    for item in payload.get("included", []) if isinstance(payload, dict) else []:
        a = item.get("attributes") or {}
        included[item.get("id")] = a
    out = []
    for item in payload.get("data", []) if isinstance(payload, dict) else []:
        a = item.get("attributes") or {}; rel = item.get("relationships") or {}
        base_id = (((rel.get("base_token") or {}).get("data") or {}).get("id"))
        tok = included.get(base_id, {})
        token_address = tok.get("address") or _gecko_token_id_to_address(base_id)
        pair_address = a.get("address") or item.get("id")
        if not token_address or not pair_address: continue
        created_ms = None
        created = a.get("pool_created_at")
        if created:
            try: created_ms = int(datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp()*1000)
            except Exception: pass
        tx = (a.get("transactions") or {}).get("h24") or {}
        vol = (a.get("volume_usd") or {}).get("h24") or 0
        pc = (a.get("price_change_percentage") or {}).get("h24") or 0
        out.append({
            "chainId": chain, "pairAddress": pair_address,
            "baseToken": {"address": token_address, "name": tok.get("name") or a.get("name") or "Unknown", "symbol": tok.get("symbol") or "UNKNOWN"},
            "liquidity": {"usd": a.get("reserve_in_usd") or 0}, "volume": {"h24": vol},
            "txns": {"h24": {"buys": tx.get("buys",0), "sells": tx.get("sells",0)}},
            "priceChange": {"h24": pc}, "pairCreatedAt": created_ms,
            "url": f"https://www.geckoterminal.com/{network}/pools/{pair_address}", "discoverySource":"geckoterminal"
        })
    return out, f"ok:{len(out)}"


async def get_token_pairs(session, chain, address):
    """DexScreener enrichment with bounded retry and per-token cache fallback."""
    key = (str(chain).lower(), str(address).lower())
    url = f"https://api.dexscreener.com/token-pairs/v1/{chain}/{address}"
    for attempt in range(2):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=18),
                headers={"User-Agent": f"MemecoinScanner/{APP_VERSION}", "Accept": "application/json"}) as response:
                if response.status == 200:
                    data = await response.json(content_type=None)
                    pairs = data if isinstance(data, list) else (data.get("pairs", []) if isinstance(data, dict) else [])
                    if pairs:
                        token_pair_cache[key] = (time.time(), pairs)
                    return pairs
                if response.status != 429: break
        except Exception:
            pass
        await asyncio.sleep(1.0 * (attempt + 1))
    cached = token_pair_cache.get(key)
    if cached and time.time() - cached[0] <= PAIR_CACHE_TTL:
        return cached[1]
    return []


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

def _extract_mints_from_parsed_tx(tx):
    """Return non-native SPL token mints touched by a parsed Solana transaction."""
    if not isinstance(tx, dict): return []
    meta = tx.get("meta") or {}
    mints = []
    for side in ("preTokenBalances", "postTokenBalances"):
        for row in meta.get(side) or []:
            mint = row.get("mint")
            if mint and mint not in {"So11111111111111111111111111111111111111112"}:
                mints.append(mint)
    return list(dict.fromkeys(mints))


async def get_raydium_pairs_for_mint(session, mint):
    """Resolve a discovered Solana mint to Raydium pools without DexScreener.
    Uses Raydium API v3 /pools/info/mint and normalizes results to the scanner pair shape.
    """
    url = "https://api-v3.raydium.io/pools/info/mint"
    params = {"mint1": mint, "poolType": "all", "poolSortField": "liquidity",
              "sortType": "desc", "pageSize": "5", "page": "1"}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=3.5),
                               headers={"Accept":"application/json","User-Agent":f"MemecoinScanner/{APP_VERSION}"}) as r:
            if r.status != 200:
                return [], f"http:{r.status}"
            body = await r.json(content_type=None)
    except asyncio.TimeoutError:
        return [], "timeout"
    except Exception as exc:
        return [], f"error:{type(exc).__name__}"
    if not isinstance(body, dict) or not body.get("success"):
        return [], "api_error"
    data = body.get("data") or {}
    rows = data.get("data") if isinstance(data, dict) else data
    if not isinstance(rows, list): rows = []
    out=[]
    for row in rows:
        if not isinstance(row, dict): continue
        ma=row.get("mintA") or {}; mb=row.get("mintB") or {}
        a_addr=ma.get("address") or ma.get("mint") or row.get("mintAAddress")
        b_addr=mb.get("address") or mb.get("mint") or row.get("mintBAddress")
        base = ma if a_addr == mint else (mb if b_addr == mint else ma)
        base_addr = base.get("address") or base.get("mint") or mint
        day=row.get("day") or row.get("stats24h") or {}
        vol=day.get("volume") if isinstance(day,dict) else None
        if vol is None: vol=row.get("volume24h") or row.get("volume24H") or 0
        txc=day.get("txCount") if isinstance(day,dict) else None
        if txc is None: txc=row.get("txCount24h") or row.get("txCount") or 0
        buys=(day.get("buyCount") or day.get("buys") or 0) if isinstance(day,dict) else 0
        sells=(day.get("sellCount") or day.get("sells") or 0) if isinstance(day,dict) else 0
        if not buys and not sells and txc: buys=int(float(txc)//2); sells=int(float(txc)-buys)
        open_time=row.get("openTime") or row.get("startTime") or row.get("createdAt")
        if isinstance(open_time,(int,float)):
            created_ms=int(open_time*1000 if open_time < 10_000_000_000 else open_time)
        else:
            created_ms=int(time.time()*1000)
        out.append({
            "chainId":"solana", "pairAddress":row.get("id") or row.get("poolId") or row.get("address"),
            "baseToken":{"address":base_addr,"name":base.get("name") or base.get("symbol") or "Unknown",
                         "symbol":base.get("symbol") or "UNKNOWN"},
            "liquidity":{"usd":row.get("tvl") or row.get("liquidity") or 0},
            "volume":{"h24":vol or 0}, "txns":{"h24":{"buys":buys,"sells":sells}},
            "priceChange":{"h24": (day.get("priceChange") or day.get("priceChangePercent") or 0) if isinstance(day,dict) else 0},
            "pairCreatedAt":created_ms, "priceUsd":row.get("price") or base.get("price") or 0,
            "url":f"https://raydium.io/swap/?inputMint=sol&outputMint={base_addr}", "_source":"raydium_v3"
        })
    return [x for x in out if x.get("pairAddress")], f"ok:{len(out)}"

async def get_direct_solana_pairs(session):
    """Fast Solana discovery path.

    Keep the live scan small: fetch a few recent signatures per launch/AMM program,
    parse only a bounded number of transactions concurrently, then enrich only the
    first fresh mints. This is intentionally incremental; successful pairs are
    persisted by discover_candidates and reused on later scans.
    """
    now = time.time()
    sigs = [(sig, ts) for sig, ts in solana_direct_signatures.items()
            if now-ts <= SOLANA_DIRECT_TTL_SECONDS]
    health=[]

    async def fetch_program(program):
        rows, source, errors = await solana_rpc_with_fallback(
            session, "getSignaturesForAddress",
            [program,{"limit":SOLANA_DIRECT_SIGS_PER_PROGRAM}])
        return program, rows, errors

    program_results = await asyncio.gather(
        *(fetch_program(p) for p in SOLANA_PROGRAM_IDS), return_exceptions=True)
    for item in program_results:
        if isinstance(item, Exception):
            health.append("program:error"); continue
        program, rows, errors = item
        if isinstance(rows,list):
            health.append(f"{program[:5]}:ok:{len(rows)}")
            for r in rows:
                sig=r.get("signature"); bt=r.get("blockTime") or int(now)
                if sig and now-bt <= SOLANA_DIRECT_TTL_SECONDS: sigs.append((sig,bt))
        else:
            health.append(f"{program[:5]}:rpc-error")

    # Deduplicate while preserving newest-first input order.
    dedup=[]; seen=set()
    for sig,ts in sigs:
        if sig not in seen:
            seen.add(sig); dedup.append((sig,ts))
    sigs=dedup[:SOLANA_DIRECT_TX_LIMIT]

    async def fetch_tx(sig):
        tx, source, errors = await solana_rpc_with_fallback(
            session,"getTransaction",
            [sig,{"encoding":"jsonParsed","maxSupportedTransactionVersion":0,"commitment":"confirmed"}])
        return tx

    txs = await asyncio.gather(*(fetch_tx(sig) for sig,_ in sigs), return_exceptions=True)
    mints=[]
    for tx in txs:
        if isinstance(tx, Exception): continue
        for mint in _extract_mints_from_parsed_tx(tx):
            if mint not in mints: mints.append(mint)
            if len(mints)>=SOLANA_DIRECT_LIMIT: break
        if len(mints)>=SOLANA_DIRECT_LIMIT: break

    # Resolve mints directly through Raydium API v3 first. DexScreener is only a fallback.
    # This breaks the previous dependency where direct on-chain discovery still needed
    # DexScreener before it could create a usable pair.
    enrich429=0; ray_health=[]
    async def enrich_mint(mint):
        ray_pairs, rh = await get_raydium_pairs_for_mint(session, mint)
        ray_health.append(rh)
        if ray_pairs:
            return ray_pairs
        try:
            return await asyncio.wait_for(get_token_pairs(session,"solana",mint), timeout=1.8)
        except asyncio.TimeoutError:
            return []
        except Exception:
            return []
    enriched = await asyncio.gather(*(enrich_mint(m) for m in mints[:8])) if mints else []
    pairs=[]
    for got in enriched:
        if got: pairs.extend(got)
    # de-duplicate pool addresses
    uniq={}
    for pair in pairs:
        if isinstance(pair,dict) and pair.get("pairAddress"):
            uniq.setdefault(pair.get("pairAddress"),pair)
    pairs=list(uniq.values())
    return pairs, {"signatures":len(sigs),"mints":len(mints),"pairs":len(pairs),
                   "enrich429":enrich429,"raydium":";".join(ray_health[:8]),"rpc":";".join(health)}

async def discover_candidates():
    """Fault-isolated discovery pipeline. Diagnostics and cache are always populated."""
    candidates = []
    stats = {
        "profiles": 0, "pair_responses": 0, "pairs": 0,
        "solana_pairs": 0, "ethereum_pairs": 0, "unsupported_chain": 0,
        "missing_data": 0, "blocked": 0, "age_missing": 0,
        "too_new": 0, "too_old": 0, "liq_low": 0, "liq_high": 0,
        "vol_low": 0, "vol_high": 0, "txns_low": 0,
        "price_change_high": 0, "meme_filter": 0, "passed": 0,
        "pair_errors": 0, "buyer_precheck": 0, "ultra_early_precheck": 0,
        "source_health": {"pipeline": "started"},
    }
    source_health = stats["source_health"]

    # Load cache before touching the network, so provider failure can never erase fallback data.
    try:
        cached_pairs = load_persistent_pairs()
        source_health["persistent_cache"] = f"loaded:{len(cached_pairs)}"
    except Exception as exc:
        cached_pairs = []
        source_health["persistent_cache"] = f"error:{type(exc).__name__}"

    async def safe(label, coro, timeout, fallback):
        try:
            value = await asyncio.wait_for(coro, timeout=timeout)
            return value, None
        except asyncio.TimeoutError:
            return fallback, f"timeout:{timeout}s"
        except Exception as exc:
            return fallback, f"error:{type(exc).__name__}"

    live_pairs = []
    profiles = []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        tasks = [
            asyncio.create_task(safe("gecko_solana", get_gecko_new_pools(session,"solana","solana"), 6, ([],"timeout"))),
            asyncio.create_task(safe("gecko_ethereum", get_gecko_new_pools(session,"eth","ethereum"), 6, ([],"timeout"))),
            asyncio.create_task(safe("dex", get_discovery_profiles(session), 8, ([],{}))),
            asyncio.create_task(safe("solana_direct", get_direct_solana_pairs(session), 12, ([],{"signatures":0,"mints":0,"pairs":0,"enrich429":0,"rpc":"timeout"}))),
        ]
        sol_res, eth_res, dex_res, direct_res = await asyncio.gather(*tasks)
        (sol_pack, sol_err), (eth_pack, eth_err), (dex_pack, dex_err), (direct_pack, direct_err) = sol_res, eth_res, dex_res, direct_res
        sol_gt, sol_health = sol_pack
        eth_gt, eth_health = eth_pack
        profiles, dex_health = dex_pack
        direct_sol, direct_health = direct_pack
        source_health.update(dict(dex_health or {}))
        source_health["gecko_solana"] = sol_err or sol_health
        source_health["gecko_ethereum"] = eth_err or eth_health
        source_health["dex_watchdog"] = dex_err or "ok"
        source_health["solana_direct"] = direct_err or f"sigs:{direct_health.get('signatures',0)} mints:{direct_health.get('mints',0)} pairs:{direct_health.get('pairs',0)} ray:{direct_health.get('raydium','n/a')} enrich429:{direct_health.get('enrich429',0)}"
        stats["profiles"] = len(profiles or [])
        live_pairs = (direct_sol or []) + (sol_gt or []) + (eth_gt or [])

        if live_pairs:
            try:
                save_persistent_pairs(live_pairs)
                source_health["cache_write"] = f"saved:{len(live_pairs)}"
            except Exception as exc:
                source_health["cache_write"] = f"error:{type(exc).__name__}"

        merged_pairs = {}
        for pair in live_pairs + cached_pairs:
            if isinstance(pair, dict):
                key = ((pair.get("chainId") or "").lower(), pair.get("pairAddress") or "")
                if key[1]: merged_pairs.setdefault(key, pair)
        results = [[p] for p in merged_pairs.values()]

        # Enrich profiles concurrently; sequential 3s waits previously exceeded the outer watchdog.
        async def enrich(profile):
            chain=profile.get("chainId"); address=profile.get("tokenAddress")
            if not chain or not address: return [], "missing"
            return await safe("pair", get_token_pairs(session,chain,address), 2.5, [])
        enrich_results = await asyncio.gather(*(enrich(p) for p in (profiles or [])[:6])) if profiles else []
        for pairs, err in enrich_results:
            if err:
                if err == "missing": stats["missing_data"] += 1
                else: stats["pair_errors"] += 1
            if pairs: results.append(pairs)
        stats["pair_responses"] = len(results)
        source_health["pipeline"] = "providers_done"
        for pairs in results:
            for pair in pairs or []:
                stats["pairs"] += 1
                try:
                    chain=(pair.get("chainId") or "").lower()
                    if chain=="solana": stats["solana_pairs"] += 1
                    elif chain=="ethereum": stats["ethereum_pairs"] += 1
                    else: stats["unsupported_chain"] += 1; continue
                    pair_address=pair.get("pairAddress"); base=pair.get("baseToken") or {}
                    name=base.get("name","Unknown"); symbol=base.get("symbol","UNKNOWN"); token_address=base.get("address")
                    if not pair_address or not token_address: stats["missing_data"] += 1; continue
                    if is_blocked_token(symbol,name): stats["blocked"] += 1; continue
                    liquidity=clean_number((pair.get("liquidity") or {}).get("usd")); volume=clean_number((pair.get("volume") or {}).get("h24"))
                    tx=(pair.get("txns") or {}).get("h24") or {}; buys=int(clean_number(tx.get("buys"))); sells=int(clean_number(tx.get("sells"))); total=buys+sells
                    price_change=clean_number((pair.get("priceChange") or {}).get("h24")); age_hours=pair_age_hours(pair)
                    if age_hours is None: stats["age_missing"] += 1; continue
                    # Strict market gate remains unchanged for real TOP-EARLY alerts.
                    strict_pass = True
                    if age_hours*60 < MIN_PAIR_AGE_MINUTES: stats["too_new"] += 1; strict_pass = False
                    elif age_hours > MAX_PAIR_AGE_HOURS: stats["too_old"] += 1; strict_pass = False
                    elif liquidity < MIN_LIQUIDITY_USD: stats["liq_low"] += 1; strict_pass = False
                    elif liquidity > MAX_LIQUIDITY_USD: stats["liq_high"] += 1; strict_pass = False
                    elif volume < MIN_VOLUME_24H: stats["vol_low"] += 1; strict_pass = False
                    elif volume > MAX_VOLUME_24H: stats["vol_high"] += 1; strict_pass = False
                    elif total < MIN_TXNS_24H: stats["txns_low"] += 1; strict_pass = False
                    elif price_change > MAX_PRICE_CHANGE_24H: stats["price_change_high"] += 1; strict_pass = False
                    elif not meme_signal(name,symbol): stats["meme_filter"] += 1; strict_pass = False

                    # Buyer precheck: inspect a small set of young Solana pairs even when the
                    # strict market gate rejects them. They can NEVER become an alert unless
                    # strict_pass is true later in perform_scan.
                    # Diagnostic precheck. Ultra-new Solana pools (< MIN_PAIR_AGE_MINUTES)
                    # are intentionally allowed through even before liquidity/volume mature.
                    # This path can NEVER alert because strict_market_pass remains False.
                    is_ultra_new = chain == "solana" and age_hours * 60 < MIN_PAIR_AGE_MINUTES
                    regular_precheck = (chain == "solana" and age_hours <= 24 and liquidity >= 1000 and volume >= 1000 and total >= 10 and meme_signal(name,symbol))
                    # Ultra-early pools often have incomplete/lagging 24h txn stats in
                    # cached/provider payloads. Requiring total >= 1 here prevented exactly
                    # those pools counted as `too_new` from ever reaching the buyer precheck.
                    # Pair/token addresses are sufficient for the on-chain diagnostic stage.
                    ultra_precheck = is_ultra_new
                    precheck_pass = regular_precheck or ultra_precheck
                    if not strict_pass and not precheck_pass:
                        continue
                    candidates.append({"chain":chain,"address":token_address,"pair_address":pair_address,"name":name,"symbol":symbol,"liquidity":liquidity,"volume":volume,"buys":buys,"sells":sells,"txns":total,"price_change":price_change,"age_hours":age_hours,"price_usd":clean_number(pair.get("priceUsd")),"url":pair.get("url"),"strict_market_pass":strict_pass,"buyer_precheck_only":not strict_pass,"ultra_early_precheck":bool(ultra_precheck and not strict_pass)})
                    if strict_pass:
                        stats["passed"] += 1
                    else:
                        stats["buyer_precheck"] += 1
                        if ultra_precheck: stats["ultra_early_precheck"] += 1
                except Exception:
                    stats["pair_errors"] += 1

    # v3.5.8: dedupe by token mint, not only pair address. Multiple pools for the
    # same token must not consume several of the three buyer-precheck slots.
    def pair_quality(c):
        # Prefer a strict-market pair first; otherwise prefer useful market data.
        # Ultra-early status is handled separately when choosing prechecks.
        return (
            1 if c.get("strict_market_pass") else 0,
            clean_number(c.get("liquidity")),
            clean_number(c.get("volume")),
            int(c.get("txns") or 0),
            -clean_number(c.get("age_hours")),
        )

    by_token = {}
    for c in candidates:
        token_key = (c.get("chain"), c.get("address"))
        current = by_token.get(token_key)
        if current is None or pair_quality(c) > pair_quality(current):
            by_token[token_key] = c

    strict = [c for c in by_token.values() if c.get("strict_market_pass")]
    precheck_pool = [c for c in by_token.values() if c.get("buyer_precheck_only")]
    precheck = sorted(
        precheck_pool,
        key=lambda c: (
            0 if c.get("ultra_early_precheck") else 1,
            c.get("age_hours", 999),
            -clean_number(c.get("liquidity")),
            -clean_number(c.get("volume")),
            -int(c.get("txns") or 0),
        ),
    )[:3]
    stats["passed_unique"] = len(strict)
    stats["buyer_precheck_selected"] = len(precheck)
    stats["token_dedupe_removed"] = max(0, len(candidates) - len(by_token))
    return strict + precheck, stats



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


async def solana_rpc_with_fallback(session, method, params):
    """Rate-aware Solana RPC transport.

    Uses low concurrency, skips endpoints during a short 429 cooldown and moves
    to the fallback instead of immediately hammering the same public endpoint.
    One retry is retained for non-429 transient failures.
    """
    urls = []
    for url in [SOLANA_RPC] + SOLANA_RPC_FALLBACKS:
        if url and url not in urls:
            urls.append(url)
    errors = []
    async with solana_rpc_semaphore:
        for url in urls:
            wait = solana_rpc_cooldown_until.get(url, 0) - time.monotonic()
            if wait > 0:
                # Do not stall a buyer scan behind a rate-limited endpoint; try fallback.
                continue
            for attempt in range(2):
                payload = {"jsonrpc":"2.0","id":1,"method":method,"params":params}
                try:
                    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=RPC_CALL_TIMEOUT)) as response:
                        if response.status == 429:
                            errors.append(f"{method}:HTTP 429")
                            solana_rpc_cooldown_until[url] = time.monotonic() + SOLANA_RPC_429_COOLDOWN
                            break
                        if response.status != 200:
                            errors.append(f"{method}:HTTP {response.status}")
                            break
                        data = await response.json(content_type=None)
                        if data.get("error"):
                            err = data["error"]
                            errors.append(f"{method}:RPC {err.get('code')}")
                            break
                        return data.get("result"), url, errors
                except asyncio.TimeoutError:
                    errors.append(f"{method}:timeout")
                except Exception as exc:
                    errors.append(f"{method}:{type(exc).__name__}")
                if attempt == 0:
                    await asyncio.sleep(0.35)
    return None, None, errors


async def solana_get_transaction_cached(session, signature):
    """Fetch an immutable confirmed transaction once and reuse it across candidates/scans."""
    now = time.monotonic()
    cached = solana_tx_cache.get(signature)
    if cached and now - cached[0] <= SOLANA_TX_CACHE_TTL:
        return cached[1], cached[2], [], True

    task = solana_tx_inflight.get(signature)
    owner = task is None
    if owner:
        task = asyncio.create_task(solana_rpc_with_fallback(
            session, "getTransaction",
            [signature, {"encoding":"jsonParsed", "commitment":"confirmed", "maxSupportedTransactionVersion":0}],
        ))
        solana_tx_inflight[signature] = task
    try:
        tx, source, errors = await task
        if tx is not None:
            solana_tx_cache[signature] = (time.monotonic(), tx, source)
        return tx, source, errors if owner else [], False
    finally:
        if owner:
            solana_tx_inflight.pop(signature, None)
        # Small bounded cache; confirmed transactions are immutable for our use.
        if len(solana_tx_cache) > 512:
            cutoff = time.monotonic() - SOLANA_TX_CACHE_TTL
            stale = [k for k, v in solana_tx_cache.items() if v[0] < cutoff]
            for k in stale:
                solana_tx_cache.pop(k, None)
            while len(solana_tx_cache) > 512:
                solana_tx_cache.pop(next(iter(solana_tx_cache)), None)


async def solana_early_buyers(session, candidate):
    """Bounded Solana buyer pipeline with swap-like verification.

    A wallet is only promoted from a token-receiver candidate to a verified
    buyer when the same transaction shows target-token inflow AND either
    native SOL spend or stablecoin spend by that wallet. This intentionally
    rejects plain token transfers and most pool/program balance movements.
    """
    result = {
        "buyers": [], "buyer_count": 0, "earliest_minutes": None,
        "status": "⚪ Keine Daten", "rpc_source": None, "rpc_errors": [],
        "signatures_found": 0, "transactions_parsed": 0,
        "tx_attempted": 0, "tx_skipped": 0,
        "wallet_candidates": 0, "token_inflows": 0,
        "swap_verified": 0, "rejected_no_payment": 0,
        "strong_buyers": 0, "dust_buyers": 0, "meaningful_buyers": 0,
    }
    pair = candidate.get("pair_address") or ""
    mint = candidate.get("address") or ""
    stable_mints = {
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    }

    async def fetch_sigs(address):
        if not address:
            return [], None, []
        try:
            return await asyncio.wait_for(
                solana_rpc_with_fallback(
                    session, "getSignaturesForAddress",
                    [address, {"limit": SOLANA_SIGNATURE_LIMIT, "commitment": "confirmed"}],
                ), timeout=max(5, RPC_CALL_TIMEOUT + 2)
            )
        except asyncio.TimeoutError:
            return [], None, ["getSignaturesForAddress:stage_timeout"]

    sig_results = await asyncio.gather(*(fetch_sigs(a) for a in (pair, mint) if a))
    signature_infos, seen = [], set()
    for rows, source, errors in sig_results:
        result["rpc_errors"].extend(errors or [])
        if source: result["rpc_source"] = source
        for row in rows or []:
            sig = row.get("signature")
            if sig and sig not in seen:
                seen.add(sig); signature_infos.append(row)

    result["signatures_found"] = len(signature_infos)
    if not signature_infos:
        result["status"] = "🟠 Solana-RPC ohne verwertbare Daten" if result["rpc_errors"] else "⚪ Keine Pool-/Token-Transaktionen"
        return result

    signature_infos.sort(key=lambda x: x.get("blockTime") or 0)
    signature_infos = signature_infos[:SOLANA_SIGNATURE_LIMIT]
    result["tx_attempted"] = len(signature_infos)

    async def fetch_tx(info):
        sig = info.get("signature")
        if not sig: return info, None, None, []
        try:
            tx, source, errors, _cached = await asyncio.wait_for(
                solana_get_transaction_cached(session, sig),
                timeout=max(5, RPC_CALL_TIMEOUT * 2 + 2)
            )
            return info, tx, source, errors
        except asyncio.TimeoutError:
            return info, None, None, ["getTransaction:stage_timeout"]

    tx_results = await asyncio.gather(*(fetch_tx(info) for info in signature_infos))
    found = {}
    wallet_candidates = set()

    for signature_info, tx, source, errors in tx_results:
        result["rpc_errors"].extend(errors or [])
        if source: result["rpc_source"] = source
        if not tx:
            result["tx_skipped"] += 1
            continue
        result["transactions_parsed"] += 1
        meta = tx.get("meta") or {}
        if meta.get("err"): continue

        pre = token_balance_map(meta.get("preTokenBalances"), mint)
        post = token_balance_map(meta.get("postTokenBalances"), mint)
        block_time = tx.get("blockTime") or signature_info.get("blockTime")
        signature = signature_info.get("signature")

        # Parse account keys + signer status for native SOL spend verification.
        message = ((tx.get("transaction") or {}).get("message") or {})
        raw_keys = message.get("accountKeys") or []
        keys, signers = [], set()
        for item in raw_keys:
            if isinstance(item, dict):
                pubkey = str(item.get("pubkey") or "")
                if pubkey:
                    keys.append(pubkey)
                    if item.get("signer"): signers.add(pubkey)
            else:
                keys.append(str(item))
        pre_lamports = meta.get("preBalances") or []
        post_lamports = meta.get("postBalances") or []
        sol_spend = {}
        for i, wallet in enumerate(keys):
            if i < len(pre_lamports) and i < len(post_lamports):
                delta = (post_lamports[i] - pre_lamports[i]) / 1_000_000_000
                if delta < -0.000005:
                    sol_spend[wallet] = -delta

        # Stablecoin spend by owner in the same transaction.
        stable_spend = {}
        for smint in stable_mints:
            spre = token_balance_map(meta.get("preTokenBalances"), smint)
            spost = token_balance_map(meta.get("postTokenBalances"), smint)
            for owner in set(spre) | set(spost):
                delta = spost.get(owner, 0) - spre.get(owner, 0)
                if delta < 0:
                    stable_spend[owner] = stable_spend.get(owner, 0) + (-delta)

        for owner in set(pre) | set(post):
            delta = post.get(owner, 0) - pre.get(owner, 0)
            if delta <= 0 or not owner: continue
            if pair and owner.lower() == pair.lower(): continue
            wallet_candidates.add(owner)
            result["token_inflows"] += 1

            # Require wallet authority/signature plus visible payment leg.
            paid_sol = sol_spend.get(owner, 0)
            paid_stable = stable_spend.get(owner, 0)
            if owner not in signers or (paid_sol <= 0 and paid_stable <= 0):
                result["rejected_no_payment"] += 1
                continue

            current = found.get(owner)
            row = {
                "wallet": owner, "amount": delta, "block_time": block_time,
                "signature": signature, "sol_spent": paid_sol,
                "stable_spent": paid_stable, "verified_swap": True,
            }
            if current is None:
                found[owner] = row
            else:
                current["amount"] += delta
                current["sol_spent"] += paid_sol
                current["stable_spent"] += paid_stable
                if block_time and (not current.get("block_time") or block_time < current["block_time"]):
                    current["block_time"] = block_time; current["signature"] = signature

    result["wallet_candidates"] = len(wallet_candidates)
    result["swap_verified"] = len(found)
    if not found:
        result["status"] = "⚪ Token-Zuflüsse erkannt, aber keine verifizierten Käufe" if result["token_inflows"] else ("⚪ Transaktionen da, aber keine Käufer-Wallets erkannt" if result["transactions_parsed"] else "🟠 Transaktions-RPC ohne verwertbare Daten")
        return result

    ordered = sorted(found.values(), key=lambda x: x.get("block_time") or 0)
    # Buyer intelligence is diagnostic only: the existing >=2 verified-buyer gate stays unchanged.
    # Thresholds are deliberately simple and transparent; they do not imply profitability.
    for rank, row in enumerate(ordered, 1):
        sol = float(row.get("sol_spent", 0) or 0)
        stable = float(row.get("stable_spent", 0) or 0)
        row["early_rank"] = rank
        if (0 < sol < 0.01) or (sol <= 0 and 0 < stable < 2):
            row["buyer_strength"] = "dust"
            result["dust_buyers"] += 1
        elif sol >= 0.10 or stable >= 20:
            row["buyer_strength"] = "strong"
            result["strong_buyers"] += 1
            result["meaningful_buyers"] += 1
        else:
            row["buyer_strength"] = "normal"
            result["meaningful_buyers"] += 1
    result["buyers"] = ordered[:10]
    result["buyer_count"] = len(ordered)
    earliest = next((x.get("block_time") for x in ordered if x.get("block_time")), None)
    if earliest: result["earliest_minutes"] = max(0, int((time.time() - earliest) / 60))
    result["status"] = "🟢 Verifizierte frühe Swap-Käufer erkannt"
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

        candidate["early_buyers"] = await early_buyer_analysis(session, candidate)

        if candidate["chain"].lower() == "solana":
            try:
                candidate["onchain_risk"] = await asyncio.wait_for(
                    solana_risk_check(session, candidate["address"]), timeout=5
                )
            except asyncio.TimeoutError:
                candidate["onchain_risk"] = "⚪ Nicht verfügbar (Timeout)"
        else:
            candidate["onchain_risk"] = "⚪ Nicht verfügbar"

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

async def perform_scan(progress=None):

    global last_scan_cache
    stage_diag = {"stage": "discovery", "candidate_timeouts": 0, "candidate_errors": 0}

    async def report(label):
        stage_diag["stage"] = label
        print("[SCAN STAGE]", label)
        if progress:
            try:
                await progress(label)
            except Exception as exc:
                print("[SCAN PROGRESS ERROR]", type(exc).__name__, str(exc))

    await report("Discovery läuft")
    try:
        raw, diagnostics = await asyncio.wait_for(discover_candidates(), timeout=DISCOVERY_STAGE_TIMEOUT)
    except asyncio.TimeoutError:
        raw, diagnostics = [], {"source_health": {"pipeline": f"outer_timeout:{DISCOVERY_STAGE_TIMEOUT}s"}}
        stage_diag["stage"] = "discovery_timeout"

    diagnostics = diagnostics or {}
    diagnostics["watchdog"] = stage_diag
    await report(f"Discovery fertig: {len(raw)} Markt-Kandidaten")

    if not raw:
        return {"checked": 0, "analyzed": 0, "candidates": [], "diagnostics": diagnostics}

    analyzed = []
    buyer_diag = {"rejected_no_buyers": 0, "signatures": 0, "transactions": 0, "rpc_errors": 0, "rpc_429": 0, "rpc_timeout": 0, "sig_errors": 0, "tx_errors": 0, "tx_attempted": 0, "tx_skipped": 0, "wallet_candidates": 0, "token_inflows": 0, "swap_verified": 0, "rejected_no_payment": 0, "strong_buyers": 0, "dust_buyers": 0, "meaningful_buyers": 0, "per_coin": []}

    for idx, candidate in enumerate(raw, 1):
        await report(f"Buyer-Analyse {idx}/{len(raw)}")
        try:
            result = await asyncio.wait_for(analyze_candidate(candidate), timeout=CANDIDATE_ANALYSIS_TIMEOUT)
        except asyncio.TimeoutError:
            stage_diag["candidate_timeouts"] += 1
            print("[CANDIDATE WATCHDOG] timeout", candidate.get("symbol") or candidate.get("name"))
            continue
        except Exception as e:
            stage_diag["candidate_errors"] += 1
            print("[ANALYZE ERROR]", type(e).__name__, str(e))
            continue

        eb = result.get("early_buyers", {}) or {}
        bc = int(eb.get("buyer_count", 0) or 0)
        buyer_diag["signatures"] += int(eb.get("signatures_found", 0) or 0)
        buyer_diag["transactions"] += int(eb.get("transactions_parsed", 0) or 0)
        buyer_diag["tx_attempted"] += int(eb.get("tx_attempted", 0) or 0)
        buyer_diag["tx_skipped"] += int(eb.get("tx_skipped", 0) or 0)
        buyer_diag["wallet_candidates"] += int(eb.get("wallet_candidates", 0) or 0)
        buyer_diag["token_inflows"] += int(eb.get("token_inflows", 0) or 0)
        buyer_diag["swap_verified"] += int(eb.get("swap_verified", 0) or 0)
        buyer_diag["rejected_no_payment"] += int(eb.get("rejected_no_payment", 0) or 0)
        buyer_diag["strong_buyers"] += int(eb.get("strong_buyers", 0) or 0)
        buyer_diag["dust_buyers"] += int(eb.get("dust_buyers", 0) or 0)
        buyer_diag["meaningful_buyers"] += int(eb.get("meaningful_buyers", 0) or 0)
        errs = eb.get("rpc_errors", []) or []
        buyer_diag["per_coin"].append({
            "name": result.get("name") or result.get("symbol") or "?",
            "symbol": result.get("symbol") or "?",
            "strict_market_pass": bool(result.get("strict_market_pass")),
            "signatures": int(eb.get("signatures_found", 0) or 0),
            "tx_attempted": int(eb.get("tx_attempted", 0) or 0),
            "transactions": int(eb.get("transactions_parsed", 0) or 0),
            "wallet_candidates": int(eb.get("wallet_candidates", 0) or 0),
            "token_inflows": int(eb.get("token_inflows", 0) or 0),
            "swap_verified": int(eb.get("swap_verified", 0) or 0),
            "strong_buyers": int(eb.get("strong_buyers", 0) or 0),
            "dust_buyers": int(eb.get("dust_buyers", 0) or 0),
            "meaningful_buyers": int(eb.get("meaningful_buyers", 0) or 0),
            "buyer_count": bc,
            "gate": "PASS" if bc >= 2 else "REJECT",
        })
        buyer_diag["rpc_errors"] += len(errs)
        buyer_diag["rpc_429"] += sum("HTTP 429" in e for e in errs)
        buyer_diag["rpc_timeout"] += sum("timeout" in e.lower() for e in errs)
        buyer_diag["sig_errors"] += sum("getSignaturesForAddress" in e for e in errs)
        buyer_diag["tx_errors"] += sum("getTransaction" in e for e in errs)
        if bc < 2:
            buyer_diag["rejected_no_buyers"] += 1
            continue
        # A precheck-only pair is diagnostic only and can never enter TOP-EARLY.
        if result.get("strict_market_pass") and result["score"] >= ALERT_SCORE:
            analyzed.append(result)

    analyzed.sort(key=lambda x: (x["score"], x["early_buyers"]["buyer_count"], -x["age_hours"]), reverse=True)
    verified = []
    for item in analyzed:
        if is_verified_early_candidate(item):
            verified.append(item)

    await report("Auswertung fertig")
    diagnostics["buyer_diag"] = buyer_diag
    diagnostics["watchdog"] = stage_diag
    last_scan_cache = verified[:10]
    return {"checked": len(raw), "analyzed": len(raw), "candidates": verified[:5], "diagnostics": diagnostics}


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
        extra = ""
        if "signatures_found" in data:
            extra += f"\n   RPC-Signaturen: {data.get('signatures_found', 0)} | TX geparst: {data.get('transactions_parsed', 0)}"
        if data.get("rpc_source"):
            extra += "\n   RPC-Fallback: aktiv"
        if data.get("rpc_errors"):
            extra += f"\n   RPC-Fehler: {len(data.get('rpc_errors', []))}"
        return (f"🐳 Early Buyers: {status}\n"
                f"   Erkannte Wallets: {count}{extra}")

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
            payment = ""
            sol_spent = float(buyer.get("sol_spent", 0) or 0)
            stable_spent = float(buyer.get("stable_spent", 0) or 0)
            if sol_spent > 0:
                payment = f" | -{sol_spent:.4f} SOL"
            elif stable_spent > 0:
                payment = f" | -{stable_spent:.2f} USDC/USDT"
            strength = buyer.get("buyer_strength", "normal")
            strength_label = {"strong": "💪 stark", "dust": "🫧 Dust", "normal": "✓ normal"}.get(strength, "✓ normal")
            early_rank = buyer.get("early_rank")
            rank_label = f" | Early #{early_rank}" if early_rank else ""
            text += (
                f"   {index}. "
                f"{short_wallet}"
                f" | +{amount:.4f} Token"
                f"{payment} | {strength_label}{rank_label}\n"
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


def verified_buyer_wallets(candidate):
    """Return unique concrete buyer wallets from the on-chain result only."""
    eb = candidate.get("early_buyers") or {}
    seen = set()
    wallets = []
    for buyer in eb.get("buyers") or []:
        if not isinstance(buyer, dict):
            continue
        wallet = str(buyer.get("wallet", "")).strip()
        if not wallet or wallet in seen:
            continue
        seen.add(wallet)
        wallets.append(wallet)
    return wallets


def is_verified_early_candidate(candidate):
    eb = candidate.get("early_buyers") or {}
    wallets = verified_buyer_wallets(candidate)
    # Never trust a stale/derived count more than the concrete wallet list.
    return int(eb.get("buyer_count", 0) or 0) >= 2 and len(wallets) >= 2


# ============================================================
# FORMAT CANDIDATE
# ============================================================

def format_candidate(
    index,
    candidate
):
    eb = candidate.get("early_buyers") or {}
    if not is_verified_early_candidate(candidate):
        raise ValueError("unverified candidate blocked from TOP-EARLY output")

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
        f"   {format_early_buyers(eb)}\n"
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


def format_per_coin_buyer_diag(stats):
    rows = stats.get("buyer_diag", {}).get("per_coin", []) or []
    if not rows:
        return "\nCoin-Details: keine Buyer-Analyse ausgeführt"
    lines = ["\nCoin-Details:"]
    for row in rows[:8]:
        market = "Markt✓" if row.get("strict_market_pass") else "Precheck"
        gate = "Gate✓" if row.get("gate") == "PASS" else "Gate✗"
        lines.append(
            f"• {row.get('name','?')} ({row.get('symbol','?')}): "
            f"Sig {row.get('signatures',0)} | TX {row.get('transactions',0)}/{row.get('tx_attempted',0)} | "
            f"Wallets {row.get('wallet_candidates',0)} | Swap-Buyer {row.get('swap_verified',0)} "
            f"| Stark {row.get('strong_buyers',0)} | Dust {row.get('dust_buyers',0)} | {market} | {gate}"
        )
    return "\n".join(lines)


def format_diagnostics(stats):
    return (
        "\n\n🧪 Discovery-Diagnose:\n"
        f"Discovery-Quellen: " + " | ".join(f"{k}={v}" for k, v in stats.get("source_health", {}).items()) + "\n"
        f"Profile geladen: {stats.get('profiles', 0)}\n"
        f"Pairs gefunden: {stats.get('pairs', 0)} "
        f"(SOL {stats.get('solana_pairs', 0)} | ETH {stats.get('ethereum_pairs', 0)})\n"
        f"Andere Chains: {stats.get('unsupported_chain', 0)}\n"
        f"Fehlende Daten/Alter: {stats.get('missing_data', 0) + stats.get('age_missing', 0)}\n"
        f"Blockiert: {stats.get('blocked', 0)}\n"
        f"Zu neu: {stats.get('too_new', 0)} | Zu alt: {stats.get('too_old', 0)}\n"
        f"Ultra-Early Precheck: {stats.get('ultra_early_precheck', 0)} | Precheck ausgewählt: {stats.get('buyer_precheck_selected', 0)} | Token-Duplikate entfernt: {stats.get('token_dedupe_removed', 0)}\n"
        f"Liquidität zu niedrig/hoch: {stats.get('liq_low', 0)}/{stats.get('liq_high', 0)}\n"
        f"Volumen zu niedrig/hoch: {stats.get('vol_low', 0)}/{stats.get('vol_high', 0)}\n"
        f"Zu wenig Txns: {stats.get('txns_low', 0)}\n"
        f"Preisanstieg-Filter: {stats.get('price_change_high', 0)}\n"
        f"Meme-Filter: {stats.get('meme_filter', 0)}\n"
        f"Pair/API-Fehler: {stats.get('pair_errors', 0)}\n"
        f"Filter bestanden: {stats.get('passed_unique', stats.get('passed', 0))}\n"
        f"\n🔬 Buyer-Diagnose:\n"
        f"Signaturen geprüft: {stats.get('buyer_diag', {}).get('signatures', 0)}\n"
        f"Transaktionen versucht/geparst/übersprungen: {stats.get('buyer_diag', {}).get('tx_attempted', 0)}/{stats.get('buyer_diag', {}).get('transactions', 0)}/{stats.get('buyer_diag', {}).get('tx_skipped', 0)}\n"
        f"RPC-Fehler: {stats.get('buyer_diag', {}).get('rpc_errors', 0)}\n"
        f"↳ 429: {stats.get('buyer_diag', {}).get('rpc_429', 0)} | Timeouts: {stats.get('buyer_diag', {}).get('rpc_timeout', 0)}\n"
        f"↳ Signatur-Fehler: {stats.get('buyer_diag', {}).get('sig_errors', 0)} | TX-Fehler: {stats.get('buyer_diag', {}).get('tx_errors', 0)}\n"
        f"Wallet-Kandidaten: {stats.get('buyer_diag', {}).get('wallet_candidates', 0)} | Token-Zuflüsse: {stats.get('buyer_diag', {}).get('token_inflows', 0)}\n"
        f"Verifizierte Swap-Buyer: {stats.get('buyer_diag', {}).get('swap_verified', 0)} | Ohne Zahlungsleg verworfen: {stats.get('buyer_diag', {}).get('rejected_no_payment', 0)}\n"
        f"Buyer-Qualität: Stark {stats.get('buyer_diag', {}).get('strong_buyers', 0)} | Normal/Meaningful {stats.get('buyer_diag', {}).get('meaningful_buyers', 0)} | Dust {stats.get('buyer_diag', {}).get('dust_buyers', 0)}\n"
        f"Ohne ≥2 Buyer verworfen: {stats.get('buyer_diag', {}).get('rejected_no_buyers', 0)}"
        + format_per_coin_buyer_diag(stats)
        + f"\n⏱ Watchdog: Stage={stats.get('watchdog', {}).get('stage', '-')} | Kandidaten-Timeouts={stats.get('watchdog', {}).get('candidate_timeouts', 0)} | Fehler={stats.get('watchdog', {}).get('candidate_errors', 0)}"
    )


async def _send_scan_result(message, result):
    checked = result["checked"]
    candidates = result["candidates"]
    candidates = [c for c in candidates if is_verified_early_candidate(c)]
    diagnostic_text = format_diagnostics(result.get("diagnostics", {}))
    if not candidates:
        text = ("✅ Scan abgeschlossen.\n\n" f"🔎 Geprüft: {checked}\n" "🚨 Kandidaten: 0\n\n" "❌ Keine passenden Early-Kandidaten gefunden." + diagnostic_text)
    else:
        text = ("✅ Scan abgeschlossen.\n\n" f"🔎 Geprüft: {checked}\n" f"🚨 Kandidaten: {len(candidates)}\n\n" "🏆 TOP-EARLY-KANDIDATEN:\n\n")
        for index, candidate in enumerate(candidates, 1):
            text += format_candidate(index, candidate) + "\n\n"
        text += diagnostic_text
    try:
        await message.reply_text(text, disable_web_page_preview=True, read_timeout=45, write_timeout=45, connect_timeout=20, pool_timeout=20)
    except Exception as exc:
        print("[SCAN RESULT SEND ERROR]", type(exc).__name__, str(exc))


async def _manual_scan_worker(message):
    last_progress = {"text": None}
    async def progress(stage):
        # Print every stage to Render; Telegram only receives meaningful checkpoints.
        print("[MANUAL SCAN]", stage)
        if stage.startswith("Discovery fertig") or stage == "Auswertung fertig":
            last_progress["text"] = stage
            await message.reply_text("⏳ " + stage, read_timeout=30, write_timeout=30, connect_timeout=15, pool_timeout=15)
    try:
        async with scan_lock:
            result = await asyncio.wait_for(perform_scan(progress=progress), timeout=210)
        await _send_scan_result(message, result)
    except asyncio.TimeoutError:
        try:
            await message.reply_text("⚠️ Scan-Watchdog nach 3,5 Minuten abgebrochen. Der Bot bleibt aktiv; bitte später erneut /scan senden.", read_timeout=45, write_timeout=45, connect_timeout=20, pool_timeout=20)
        except Exception as exc:
            print("[SCAN TIMEOUT SEND ERROR]", type(exc).__name__, str(exc))
    except Exception as exc:
        print("[MANUAL SCAN ERROR]", type(exc).__name__, str(exc))
        try:
            await message.reply_text("⚠️ Scan fehlgeschlagen, der Bot läuft weiter. Bitte später erneut /scan senden.", read_timeout=45, write_timeout=45, connect_timeout=20, pool_timeout=20)
        except Exception:
            pass


async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    await update.message.reply_text(
        f"🔎 {APP_VERSION}: Scan gestartet.\n\nDie On-Chain-Analyse läuft im Hintergrund; /start und /status bleiben währenddessen verfügbar.",
        read_timeout=45, write_timeout=45, connect_timeout=20, pool_timeout=20,
    )
    task = asyncio.create_task(_manual_scan_worker(update.message))
    manual_scan_tasks.add(task)
    task.add_done_callback(manual_scan_tasks.discard)


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
                        value = ((payload.get("params") or {}).get("result") or {}).get("value") or {}
                        sig = value.get("signature")
                        if sig:
                            solana_direct_signatures[sig] = time.time()
                            # prune old entries
                            cutoff=time.time()-SOLANA_DIRECT_TTL_SECONDS
                            for k,v in list(solana_direct_signatures.items()):
                                if v < cutoff: solana_direct_signatures.pop(k,None)
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

    await asyncio.sleep(20)

    while True:

        try:

            async with scan_lock:
                result = await asyncio.wait_for(perform_scan(), timeout=240)

            candidates = result[
                "candidates"
            ]

            if candidates:
                print("[SCAN]", len(candidates), "Kandidaten gefunden.")
                for candidate in candidates:
                    buyers = candidate["early_buyers"]["buyer_count"]
                    print("[CANDIDATE]", candidate["name"], candidate["symbol"], candidate["chain"], "Score=", candidate["score"], "EarlyBuyers=", buyers)
                    if (AUTO_ALERT and ALLOWED_CHAT_ID
                            and is_verified_early_candidate(candidate)
                            and alert_is_due(candidate)):
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
        .token(TELEGRAM_BOT_TOKEN)
        .connect_timeout(20)
        .read_timeout(45)
        .write_timeout(45)
        .pool_timeout(20)
        .get_updates_connect_timeout(20)
        .get_updates_read_timeout(60)
        .get_updates_write_timeout(45)
        .get_updates_pool_timeout(20)
        .post_init(post_init)
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