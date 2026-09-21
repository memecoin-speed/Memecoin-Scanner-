import os
import asyncio
import json
import time

import aiohttp
import websockets
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("ALLOWED_CHAT_ID")

SOLANA_HTTP = os.getenv(
    "SOLANA_HTTP_URL",
    "https://api.mainnet-beta.solana.com"
)

SOLANA_WS = os.getenv(
    "SOLANA_WS_URL",
    "wss://api.mainnet-beta.solana.com"
)

MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY_USD", "5000"))
MIN_VOLUME = float(os.getenv("MIN_VOLUME_5M_USD", "1000"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "55"))

# Pump.fun Programm
PUMP_PROGRAM = os.getenv(
    "PUMP_PROGRAM_ID",
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
)

seen_tokens = set()
last_alert = {}


async def rpc(method, params):
    payload = {
        "jsonrpc": "2.0",
        "id": int(time.time() * 1000),
        "method": method,
        "params": params,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            SOLANA_HTTP,
            json=payload,
            timeout=15
        ) as response:
            return await response.json()


async def dex_data(token):
    url = (
        "https://api.dexscreener.com"
        "/latest/dex/tokens/"
        + token
    )

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=10
            ) as response:

                if response.status != 200:
                    return None

                data = await response.json()

        pairs = data.get("pairs") or []

        sol_pairs = [
            p for p in pairs
            if p.get("chainId") == "solana"
        ]

        if not sol_pairs:
            return None

        return max(
            sol_pairs,
            key=lambda p: float(
                (p.get("liquidity") or {}).get("usd") or 0
            )
        )

    except Exception as e:
        print("DexScreener Fehler:", e)
        return None


def calculate_score(pair):

    liquidity = float(
        (pair.get("liquidity") or {}).get("usd") or 0
    )

    volume5m = float(
        (pair.get("volume") or {}).get("m5") or 0
    )

    buys = int(
        (pair.get("txns") or {}).get("m5", {}).get("buys") or 0
    )

    sells = int(
        (pair.get("txns") or {}).get("m5", {}).get("sells") or 0
    )

    score = 0
    reasons = []

    if liquidity >= 25000:
        score += 25
        reasons.append("💧 hohe Liquidität")
    elif liquidity >= MIN_LIQUIDITY:
        score += 15
        reasons.append("💧 ausreichende Liquidität")
    else:
        reasons.append("⚠️ geringe Liquidität")

    if volume5m >= 10000:
        score += 20
        reasons.append("📈 hohes 5m Volumen")
    elif volume5m >= MIN_VOLUME:
        score += 10
        reasons.append("📈 Volumen vorhanden")
    else:
        reasons.append("⚠️ geringes Volumen")

    total = buys + sells

    if total >= 10:
        buy_ratio = buys / total

        if buy_ratio >= 0.60:
            score += 20
            reasons.append("🟢 Käufer dominieren")
        elif buy_ratio >= 0.50:
            score += 10
            reasons.append("🟡 leicht mehr Käufe")
        else:
            reasons.append("🔴 Verkäufer dominieren")
    else:
        reasons.append("⚠️ wenige Trades")

    age = 0

    try:
        created = pair.get("pairCreatedAt")

        if created:
            age = max(
                0,
                int(time.time() - created / 1000)
            )

            if age <= 300:
                score += 20
                reasons.append("⚡ sehr jung")

            elif age <= 1200:
                score += 10
                reasons.append("⏱️ jung")

    except Exception:
        pass

    return min(score, 100), reasons, liquidity, volume5m, buys, sells, age


async def send_alert(
    application,
    token,
    pair,
    score,
    reasons,
    liquidity,
    volume5m,
    buys,
    sells,
    age
):

    if not CHAT_ID:
        print("ALLOWED_CHAT_ID fehlt.")
        return

    now = time.time()

    if token in last_alert:
        if now - last_alert[token] < 60:
            return

    last_alert[token] = now

    symbol = pair.get("baseToken", {}).get(
        "symbol",
        "UNKNOWN"
    )

    name = pair.get("baseToken", {}).get(
        "name",
        "Unknown"
    )

    dex = pair.get("dexId", "unknown")

    chart = (
        "https://dexscreener.com/solana/"
        + token
    )

    age_text = (
        f"{age // 60}m {age % 60}s"
        if age
        else "unbekannt"
    )

    text = (
        "🚨 *NEW MEMECOIN SIGNAL*\n\n"
        f"🪙 *{name}* ({symbol})\n"
        f"📍 `{token}`\n\n"
        f"⏱ Alter: {age_text}\n"
        f"💧 Liquidität: ${liquidity:,.0f}\n"
        f"📈 5m Volumen: ${volume5m:,.0f}\n"
        f"🟢 Buys: {buys}\n"
        f"🔴 Sells: {sells}\n"
        f"🏪 DEX: {dex}\n\n"
        f"⭐ *Score: {score}/100*\n\n"
        + "\n".join(reasons)
        + f"\n\n📊 [Chart öffnen]({chart})"
    )

    await application.bot.send_message(
        chat_id=CHAT_ID,
        text=text,
        parse_mode="Markdown",
        disable_web_page_preview=False,
    )


def extract_tokens(logs):

    tokens = []

    for log in logs:

        if "InitializeMint" in log:
            parts = log.split()

            for part in parts:
                if len(part) >= 32 and len(part) <= 44:
                    tokens.append(part)

    return list(set(tokens))


async def inspect_transaction(application, signature):

    try:

        result = await rpc(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "processed",
                    "maxSupportedTransactionVersion": 0
                }
            ]
        )

        tx = result.get("result")

        if not tx:
            return

        message = (
            tx.get("transaction", {})
            .get("message", {})
        )

        instructions = message.get(
            "instructions",
            []
        )

        for instruction in instructions:

            parsed = instruction.get("parsed")

            if not parsed:
                continue

            if parsed.get("type") not in (
                "initializeMint",
                "initializeMint2"
            ):
                continue

            info = parsed.get("info", {})
            token = info.get("mint")

            if not token:
                continue

            if token in seen_tokens:
                continue

            seen_tokens.add(token)

            print("Neuer Token:", token)

            pair = await dex_data(token)

            if not pair:
                continue

            (
                score,
                reasons,
                liquidity,
                volume5m,
                buys,
                sells,
                age
            ) = calculate_score(pair)

            print(
                token,
                "Score:",
                score
            )

            if (
                score >= MIN_SCORE
                and liquidity >= MIN_LIQUIDITY
                and volume5m >= MIN_VOLUME
            ):

                await send_alert(
                    application,
                    token,
                    pair,
                    score,
                    reasons,
                    liquidity,
                    volume5m,
                    buys,
                    sells,
                    age
                )

    except Exception as e:

        print(
            "Transaction Fehler:",
            e
        )


async def solana_scanner(application):

    while True:

        try:

            print(
                "🔌 Verbinde mit Solana WebSocket..."
            )

            async with websockets.connect(
                SOLANA_WS,
                ping_interval=20,
                ping_timeout=20
            ) as ws:

                request = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {
                            "mentions": [
                                PUMP_PROGRAM
                            ]
                        },
                        {
                            "commitment": "processed"
                        }
                    ]
                }

                await ws.send(
                    json.dumps(request)
                )

                print(
                    "🟢 Solana Scanner verbunden"
                )

                async for message in ws:

                    data = json.loads(message)

                    if data.get("method") != "logsNotification":
                        continue

                    value = (
                        data.get("params", {})
                        .get("result", {})
                        .get("value", {})
                    )

                    if value.get("err"):
                        continue

                    signature = value.get(
                        "signature"
                    )

                    if signature:
                        asyncio.create_task(
                            inspect_transaction(
                                application,
                                signature
                            )
                        )

        except Exception as e:

            print(
                "WebSocket Fehler:",
                e
            )

            await asyncio.sleep(5)


async def start(update: Update, context):

    chat_id = update.effective_chat.id

    await update.message.reply_text(
        "🤖 *Memecoin Early Scanner*\n\n"
        "🟢 Telegram: ONLINE\n"
        "🟢 Solana Scanner: AKTIV\n"
        "🧪 Paper Trading: NOCH NICHT AKTIV\n\n"
        f"🆔 Chat-ID: `{chat_id}`",
        parse_mode="Markdown"
    )


async def status(update: Update, context):

    await update.message.reply_text(
        "🟢 Scanner läuft\n\n"
        f"👀 Beobachtete Tokens: {len(seen_tokens)}\n"
        f"💧 Mindest-Liquidität: ${MIN_LIQUIDITY:,.0f}\n"
        f"📈 Mindest-5m-Volumen: ${MIN_VOLUME:,.0f}\n"
        f"⭐ Mindest-Score: {MIN_SCORE}/100\n\n"
        "🧪 Paper Trading: NOCH NICHT AKTIV"
    )


async def main():

    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN fehlt."
        )

    application = (
        Application
        .builder()
        .token(TOKEN)
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

    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    scanner = asyncio.create_task(
        solana_scanner(application)
    )

    print(
        "🚀 Memecoin Early Scanner läuft!"
    )

    try:

        await asyncio.Event().wait()

    finally:

        scanner.cancel()

        await application.updater.stop()
        await application.stop()
        await application.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
