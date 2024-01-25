import traceback
import httpx, logging, random, re, json
import pandas as pd
import os
from pathlib import Path
from datetime import datetime, timedelta
from automap_fetcher import AutomapFetcher
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

THISFILE = Path(__file__)
try:
    from roads import roads as allroads
except:
    logging.error(traceback.format_exc())
    allroads = {}

logging.basicConfig(
    format='%(asctime)s - [%(name)s - %(levelname)s] [%(funcName)s l %(lineno)s] - %(message)s',
    level=logging.INFO
)

try:
    import dotenv
    dotenv.load_dotenv(".env")
except:
    pass

ISDEBUG = "DEBUG" in os.environ
TOKEN = os.environ["TOKEN"]
# MYCHATID = int(os.environ["MY_CHAT_ID"])
# ADMIN_USERNAME = os.environ["ADMIN_USERNAME"]

fetcher = AutomapFetcher()

async def message_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        userinfo = update.effective_user
        logging.info(f"Received new message from user {userinfo}")
        logging.info(f"Chat information {update.effective_chat}")
        # if userinfo.username != ADMIN_USERNAME:
        #     await context.bot.send_message(chat_id=MYCHATID, text=f"Received new message from user {userinfo}")
    except:
        logging.warning(f"Problem in retrieving user information")
    
    past_hours = 3
    ndays_next = 3
    inputmsg = update.message.text.strip().lower()
    m = re.match(r"^(a\d+)(.*)$", inputmsg)
    if m:
        try:
            args = m.groups()
            a_name = args[0].lower()
            traffic, update_time = await fetcher.getTrafficEvents(a_name)
            closures = await fetcher.getClosureEvents(a_name)
            if not update_time:
                update_time = traffic["start_date"].max()
                logging.warning("Could not parse update time from web page")
            sel = traffic["start_date"] >= (update_time - timedelta(hours=past_hours))
            traffic = traffic[sel]
            traffic_messages = fetcher.formatTrafficEvents(traffic, update_time, past_hours)
            logging.info(f"Sending {len(traffic_messages)} messages")
            for i, msg in enumerate(traffic_messages):
                await update.message.reply_text(f"{msg}\npagina {i+1}/{len(traffic_messages)}")
            
            closure_messages = fetcher.formatClosureEvents(closures, update_time, ndays_next=ndays_next)
            for i, msg in enumerate(closure_messages):
                await update.message.reply_text(f"{msg}\npagina {i+1}/{len(closure_messages)}")
        except:
            errmsg = f"An error occurred with your message: '{inputmsg}'"
            if ISDEBUG:
                errmsg += f" {traceback.format_exc()}"
            logging.error(errmsg)
            await update.message.reply_text(f"An error occurred with your message: '{inputmsg}'")
    else:
        resp_msg = "Dimmi un'autostrada, ad esempio A10, A12, A7, A1"
        await update.message.reply_text(resp_msg)


async def list_roads(update: Update, context: ContextTypes.DEFAULT_TYPE):
    resp_msg = "Lista autostrade:\n\n"
    for road in allroads:
        resp_msg += f"- {road}\n"
    await update.message.reply_text(resp_msg)

if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("lista", list_roads))
    app.add_handler(MessageHandler(filters.ALL, message_callback))
    if ISDEBUG:
        app.run_polling()
    else:    
        app.run_webhook(
            listen='0.0.0.0',
            port=os.getenv("PORT", 8080),
            url_path=TOKEN,
            webhook_url=f"{os.environ['HEROKU_URL']}/{TOKEN}"
        )