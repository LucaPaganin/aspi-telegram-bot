import traceback
import httpx, logging, random, re
import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from automap_fetcher import AutomapFetcher
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
            events = await client.get("https://viabilita.autostrade.it/json/eventi.json")
            forecasts = await client.get("https://viabilita.autostrade.it/json/previsioni.json")
        events = build_df_from_response(events)
        forecasts = build_df_from_response(forecasts)
        try:
            extract_dates(forecasts)
        except:
            pass
        tot = pd.concat([events, forecasts])
        tot = tot.reset_index()
        cache["data"] = tot
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
        for i, row in sel.iterrows():
            desc = row["t_des_it"]
            msg = f"- {desc}"
            if "start_date" in row:
                msg += f" ({row['start_date']})"
            msg += "\n"
            s += msg
    return s


def zeropad_a_name(a_name):
    res = a_name.lower().lstrip("a")
    if len(res) == 1:
        res = f"A0{res}"
    else:
        res = f"A{res}"
    return res

async def get_events(a_name):
    now = datetime.now()
    threshold = now+timedelta(days=1)
    put_threshold = True
    logging.info(f"a_name start: {a_name}")
    a_name = zeropad_a_name(a_name)
    logging.info(f"a_name final: {a_name}")
    df = await fetch_aspi_updates()
    logging.info(f"fetched {len(df)} events")
    a_df = df[df["c_str"] == a_name]
    msg = ""
    if "start_date" in df.columns:
        logging.info(f"threshold date: {threshold}")
        if put_threshold:
            a_df["start_date"] = pd.to_datetime(df["start_date"])
            a_df = a_df[(a_df["start_date"] >= now) & (a_df["start_date"] < threshold)]
            if len(a_df) > 0:
                msg = f"Eventi precedenti a {threshold.strftime('%d-%m-%Y %H:%M:%S')}\n\n"
            else:
                msg = f"Nessun evento da segnalare sulla {a_name} prima del {threshold.strftime('%d-%m-%Y %H:%M:%S')}"
        else:
            msg = "Nessun filtro di data impostato.\n\n"
    return a_df, msg


async def aspi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info(context.args)
    a_name = "A10"
    if len(context.args) >= 1:
        a_name = context.args[0].upper()
    a_df, msg = await get_events(a_name)
    fmt = format_events(a_df)
    logging.info(f"message length: {len(fmt)}")
    if fmt:
        msg += fmt
    await update.message.reply_text(msg)


fetcher = AutomapFetcher()


async def message_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message.text.strip().lower()
    if re.match(r"^a\d+$", msg):
        try:
            a_name = msg.lower()
            traffic, update_time = await fetcher.getTrafficEvents(a_name)
            closures = await fetcher.getClosureEvents(a_name)
            if not update_time:
                update_time = traffic["start_date"].max()
                logging.warning("Could not parse update time from web page")
            sel = traffic["start_date"] >= (update_time - timedelta(hours=4))
            traffic = traffic[sel]        
            resp_msg = f"Traffico: ultimo aggiornamento alle {update_time.strftime('%H:%M')}\n\n"
            resp_msg += f"Eventi da segnalare: {len(traffic)}\n\n"
            for i, row in traffic.iterrows():
                resp_msg += f"- {row['start_date']} \n\t{row['desc']}\n\n"
            resp_msg += "\n"
            await update.message.reply_text(resp_msg)
            
            resp_msg = f"Chiusure programmate: {len(closures)}\n"
            for i, row in closures.iterrows():
                resp_msg += f"- {row['start_date']} \n\t{row['desc']}\n\n"
            await update.message.reply_text(resp_msg)
        except:
            resp_msg = f"An error occurred: {traceback.format_exc()}"
            await update.message.reply_text(resp_msg)
    else:
        resp_msg = "Dimmi un'autostrada, ad esempio A10, A12, A7, A1"
        await update.message.reply_text(resp_msg)

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