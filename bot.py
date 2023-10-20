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

def _fmt_date(row):
    try:
        return datetime.strptime(f"{row['start_dd']} {row['start_hr']}", "%d/%m/%Y %H:%M").isoformat()
    except:
        return None

def extract_dates(df):
    df[["start_hr", "end_hr"]] = df['t_des_it'].str.extract("dalle ore ([\d\:]+) alle ore ([\d\:]+)")
    df[["start_dd", "end_dd"]] = df['t_des_it'].str.extract("d[ea]l giorno (\d{2}/\d{2}/\d{4}) al giorno (\d{2}/\d{2}/\d{4})")
    
    df["start_date"] = df.apply(_fmt_date, axis=1)
    df["end_date"] = df.apply(_fmt_date, axis=1)
    df = df[(~df["start_date"].isna()) & (~df["start_date"].isna())]

def build_df_from_response(resp):
    data = resp.json()
    df = pd.DataFrame(data['events'])
    df['start_date'] = df['d_agg'].apply(lambda x: datetime.fromtimestamp(x/1000).isoformat())
    sort_cols = ["c_str", "start_date"]
    df.sort_values(sort_cols, inplace=True, ignore_index=True)
    return df


async def fetch_aspi_updates():
    now = datetime.now()
    dt = now - cache["last_update"]
    if (dt <= CACHE_DURATION) or cache["data"] is None:
        logging.info("fetching updates from aspi")
        async with httpx.AsyncClient() as client:
            r = await client.get("https://viabilita.autostrade.it/json/eventi.json")
            # previsions = await client.get("https://viabilita.autostrade.it/json/previsioni.json")
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
    res = a_name.lower().lstrip("a")
    if len(res) == 1:
        res = f"A0{res}"
    else:
        res = f"A{res}"
    return res


async def aspi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(context.args)
    now = datetime.now()
    threshold = now+timedelta(days=1)
    put_threshold = True
    a_name = "A10"
    whichevs = None
    if len(context.args) >= 1:
        a_name = context.args[0].upper()
    if len(context.args) >= 2:
        whichevs = context.args[1]
    if whichevs and whichevs.lower() == "all":
        put_threshold = False
    logging.info(f"a_name start: {a_name}")
    a_name = zeropad_a_name(a_name)
    logging.info(f"a_name final: {a_name}")
    df = await fetch_aspi_updates()
    logging.info(f"fetched {len(df)} events")
    a_df = df[df["c_str"] == a_name]
    msg = ""
    if "start_date" in df.columns:
        logging.info(f"min start date: {a_df['start_date'].min()}")
        logging.info(f"max start date: {a_df['start_date'].max()}")
        logging.info(f"threshold date: {threshold}")
        if put_threshold:
            a_df["start_date"] = pd.to_datetime(df["start_date"])
            a_df = a_df[(a_df["start_date"] < threshold)]
            if len(a_df) > 0:
                msg = f"Eventi precedenti a {threshold.strftime('%d-%m-%Y %H:%M:%S')}\n\n"
            else:
                msg = f"Nessun evento da segnalare sulla {a_name} prima del {threshold.strftime('%d-%m-%Y %H:%M:%S')}"
        else:
            msg = "Nessun filtro di data impostato.\n\n"
    fmt = format_events(a_df)
    logging.info(f"message length: {len(fmt)}")
    if fmt:
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
        "non toccare teti": "quando è nero paghi scotto",
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
    
    if os.getenv("DEBUG"):
        app.run_polling()
    else:    
        app.run_webhook(
            listen='0.0.0.0',
            port=os.getenv("PORT", 8080),
            url_path=TOKEN,
            webhook_url=f"https://aspi-bot-387e664a1cf9.herokuapp.com/{TOKEN}"
        )