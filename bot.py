import httpx
import pandas as pd
import random
from datetime import datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

async def aspi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with httpx.AsyncClient() as client:
        r = await client.get("https://viabilita.autostrade.it/json/previsioni.json")
    data = r.json()
    df = pd.DataFrame(data['events'])
    df = df[df['c_str'].isin(["A10", "A07", "A26"])]
    df[["start_hr", "end_hr"]] = df['t_des_it'].str.extract("dalle ore ([\d\:]+) alle ore ([\d\:]+)")
    df[["start_dd", "end_dd"]] = df['t_des_it'].str.extract("dal giorno (\d{2}/\d{2}/\d{4}) al giorno (\d{2}/\d{2}/\d{4})")
    
    df["start_date"] = df.apply(lambda row: datetime.strptime(f"{row['start_dd']} {row['start_hr']}", "%d/%m/%Y %H:%M"), 
                            axis=1)
    df["end_date"] = df.apply(lambda row: datetime.strptime(f"{row['end_dd']} {row['end_hr']}", "%d/%m/%Y %H:%M"), 
                            axis=1)
    df.sort_values(["c_str", "start_date"], inplace=True, ignore_index=True)
    
    i = random.randint(0, len(df))
    print(f"event {i}/{len(df)}")
    msg = df.iloc[i]['t_des_it']
    
    await update.message.reply_text(msg)
    

    

async def hello(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f'Hello {update.effective_user.first_name}')


app = ApplicationBuilder().token("1969028284:AAF0oDjLtKoodTMiiWrIyTXkz8XqiJumy0w").build()

app.add_handler(CommandHandler("hello", hello))
app.add_handler(CommandHandler("aspi", aspi))



app.run_polling()