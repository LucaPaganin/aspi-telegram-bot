import httpx, logging, random
import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

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

frasi = [
    "And the winner is Kevin Teti",
    "Kevin Teti",
    "Scotto",
    "è il momento chibaku tensei",
    "Senpō: Muki Tensei",
    "Senpō: Ranton Koga",
    "Senpō: Inton Raiha",
    "Doton: Doruku Gaeshi",
    "Amaterasu",
    "Susanoo",
    "Izanami",
]

def build_df_from_response(resp):
    data = resp.json()
    df = pd.DataFrame(data['events'])
    df = df[df['c_str'].isin(["A06", "A07", "A10", "A12", "A26"])]
    df[["start_hr", "end_hr"]] = df['t_des_it'].str.extract("dalle ore ([\d\:]+) alle ore ([\d\:]+)")
    df[["start_dd", "end_dd"]] = df['t_des_it'].str.extract("dal giorno (\d{2}/\d{2}/\d{4}) al giorno (\d{2}/\d{2}/\d{4})")
    
    df["start_date"] = df.apply(lambda row: datetime.strptime(f"{row['start_dd']} {row['start_hr']}", "%d/%m/%Y %H:%M").isoformat(), 
                                axis=1)
    df["end_date"] = df.apply(lambda row: datetime.strptime(f"{row['end_dd']} {row['end_hr']}", "%d/%m/%Y %H:%M").isoformat(), 
                              axis=1)
    df.sort_values(["c_str", "start_date"], inplace=True, ignore_index=True)
    return df


async def fetch_aspi_updates():
    now = datetime.now()
    dt = now - cache["last_update"]
    if (dt <= CACHE_DURATION) or cache["data"] is None:
        logging.info("fetching updates from aspi")
        async with httpx.AsyncClient() as client:
            r = await client.get("https://viabilita.autostrade.it/json/previsioni.json")
        cache["data"] = build_df_from_response(r)
        fdata = {
            "last_update": cache["last_update"].isoformat(),
            "data": cache["data"].to_dict()
        }
        Path("cache.json").write_text(json.dumps(fdata, indent=2))
        
    return cache["data"]


def format_events(df):
    roads = df["c_str"].unique().tolist()
    s = ""
    for r in roads:
        s += f"Eventi sulla {r}:\n\n"
        sel = df[df["c_str"] == r]
        for text in sel["t_des_it"].values:
            s +=f"- {text}\n"
    return s


def zeropad_a_name(a_name):
    res = a_name.lstrip("a")
    if len(res) == 1:
        res = f"A0{res}"
    return res


async def aspi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(context.args)
    now = datetime.now()
    threshold = now+timedelta(days=1)
    a_name = "A10"
    if len(context.args) >= 1:
        a_name = context.args[0].upper()
    a_name = zeropad_a_name(a_name)
    logging.info(f"a_name final: {a_name}")
    df = await fetch_aspi_updates()
    df["start_date"] = pd.to_datetime(df["start_date"])
    sel = df[(df["start_date"] < threshold) & (df["c_str"] == a_name)]
    msg = f"Eventi precedenti a {threshold.strftime('%d-%m-%Y %H:%M:%S')}\n\n"
    fmt = format_events(sel)
    logging.info(f"message length: {len(fmt)}")
    if not fmt:
        msg = "No events to be notified"
    else:
        msg += fmt
    await update.message.reply_text(msg)

senpous = [
    "Muki Tensei",
    "Ranton Koga",
    "Inton Raiha",
    "Goemon",
    "Oodama Rasen Shuriken",
]
async def message_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message.text.strip().lower()
    logging.info(msg)
    reply = {
        "and the winner is": "Kevin Teti",
        "non toccare Teti": "quando è nero paghi scotto",
        "è il momento": "Chibaku Tensei",
        "teti": "certamente",
        "scotto": "ovviamente"
    }
    if msg == "senpo":
        resp = random.choice(senpous)
    else:    
        try:
            resp = reply[msg.lower()]
        except KeyError:
            resp = "Gatoringa"
    await update.message.reply_text(resp)

if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("aspi", aspi))
    app.add_handler(MessageHandler(filters.ALL, message_callback))

    app.run_webhook(
        listen='0.0.0.0',
        port=os.getenv("PORT", 8080),
        url_path=TOKEN,
        webhook_url=f"https://aspi-bot-387e664a1cf9.herokuapp.com/{TOKEN}"
    )