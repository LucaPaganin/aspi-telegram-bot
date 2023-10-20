import httpx, logging
import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

TOKEN = "1969028284:AAF0oDjLtKoodTMiiWrIyTXkz8XqiJumy0w"
CACHE_DURATION = timedelta(minutes=5)
GROUPID = -4080210648

cache = {
    "last_update": datetime.now() - CACHE_DURATION,
    "data": None
}

async def aspi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("stacca stacca")

if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("aspi", aspi))

    app.run_webhook(
        listen='0.0.0.0',
        port=443,
        url_path=TOKEN,
        webhook_url=f"https://aspi-bot-387e664a1cf9.herokuapp.com/{TOKEN}"
    )