import os
import asyncio
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_CHAT_ID = os.getenv("ALLOWED_CHAT_ID")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    await update.message.reply_text(
        f"🤖 Memecoin Early Scanner\n\n"
        f"✅ Bot ist online!\n"
        f"🆔 Deine Chat-ID: `{chat_id}`\n\n"
        f"Als Nächstes verbinden wir den Solana-Scanner.",
        parse_mode="Markdown",
    )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🟢 Scanner-Status\n\n"
        "Telegram: ✅\n"
        "Solana Scanner: ⏳\n"
        "Paper Trading: ⏳"
    )


def main():
    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN wurde nicht gesetzt."
        )

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))

    print("🤖 Memecoin Early Scanner gestartet")
    app.run_polling()


if __name__ == "__main__":
    main()
