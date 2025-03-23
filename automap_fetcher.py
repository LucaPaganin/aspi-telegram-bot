import httpx, re, pandas as pd, logging, os
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from utils import MONTHMAP_ITA_ENG, RoadEvent, validateRoadName, MONTHS, create_message_chunks

class AutomapFetcher(object):
    def __init__(self) -> None:
        self.last_update = None
        self._base_url = "https://automap.it"
    
    def makeSyncRequest(self, method, url, **kwargs):
        kwargs["follow_redirects"] = True
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]["User-Agent"] = "Mozilla/5.0"
        return httpx.request(method, url, **kwargs)
    
    async def makeAsyncRequest(self, method, url, **kwargs):
        kwargs["follow_redirects"] = True
        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]["User-Agent"] = "Mozilla/5.0"
        async with httpx.AsyncClient() as client:
            res = await client.request(method, url, **kwargs)
        return res
    
    async def getTrafficEvents(self, road_name):
        aname = validateRoadName(road_name)
        url = f"{self._base_url}/traffico/{aname}"
        res = await self.makeAsyncRequest("get", url)
        return self.parseTrafficEvents(res)
    
    def getTrafficEventsSync(self, road_name):
        aname = validateRoadName(road_name)
        url = f"{self._base_url}/traffico/{aname}"
        res = self.makeSyncRequest("get", url)
        return self.parseTrafficEvents(res)
    
    async def getClosureEvents(self, road_name):
        aname = validateRoadName(road_name)
        url = f"{self._base_url}/chiusure/{aname}"
        res = await self.makeAsyncRequest("get", url)
        return self.parseClosureEvents(res, road_name)
    
    def getClosuresSync(self, road_name):
        aname = validateRoadName(road_name)
        res = self.makeSyncRequest("get", f"{self._base_url}/chiusure/{aname}")
        return self.parseClosureEvents(res, road_name)
    
    def parseClosureEvents(self, res: "httpx.Response", road_name):
        soup = BeautifulSoup(res.text, 'html.parser')
        events = soup.find_all("div", attrs={"class": "evento"})
        closures = []
        for e in events:
            desc, t1, t2 = self.parseClosureEvent(e)
            rev = RoadEvent(start_date=t1, road_name=road_name, desc=desc, end_date=t2, evtype="closure")
            closures.append(rev.to_dict())
        df = self._createDf(closures, sortasc=True)
        
        return df
    
    def parseClosureEvent(self, e):
        lines = [l.strip() for l in e.get_text().splitlines() if l.strip()]
        fulltext = "".join(lines)
        t1 = None
        t2 = None
        hours = re.findall(r" (\d{2}\:\d{2}) ", fulltext)
        days  = re.findall(r" (\d{2}/\d{2}/\d{4}) ", fulltext)
        try:
            t1 = datetime.strptime(f"{days[0]} {hours[0]}", "%d/%m/%Y %H:%M")
            t2 = datetime.strptime(f"{days[1]} {hours[1]}", "%d/%m/%Y %H:%M")
        except IndexError:
            pass
        desc = "\n".join(lines)
        return desc, t1, t2
    
    def parseTrafficEvents(self, res: "httpx.Response"):
        if res.status_code != 200:
            res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        news = soup.find_all("div", attrs={"class": "avviso"})
        traffics = [
            self.parseTrafficEvent(n).to_dict() for n in news
        ]
        update_time = self._getTrafficUpdateTime(soup)
        return self._createDf(traffics, sortasc=False), update_time
    
    def _getTrafficUpdateTime(self, soup):
        el = soup.find("div", attrs={"class": "aggiorna"})
        if el:
            text = el.get_text().splitlines()[1].lower()
            parts = [p.strip() for p in text.split("-")]
            day, month, year = parts[0].split()[1:]
            hour, minute = parts[1].split()[-1].split(":")
            month_num = MONTHS.index(month)+1
            dt = datetime(int(year), month_num, int(day), int(hour), int(minute))
            return dt
    
    def parseTrafficEvent(self, a):
        children = list(a.children)
        hr = a.find("span").text
        eg = a.find("div", attrs={"class": "egiorno"})
        desc = str(children[children.index(eg)+1]).strip()
        em = a.find("em")
        sd_parts = eg.text.split(" ")[1:]
        sd_parts[1] = MONTHMAP_ITA_ENG[sd_parts[1]]
        sd_parts.insert(2, str(datetime.now().year))
        start_date = " ".join([*sd_parts, hr])
        road_name = a.find("a").text.split()[0]
        
        kwargs = {} 
        if em is not None:
            kwargs["direction"] = em.text
        return RoadEvent(start_date, road_name, desc, evtype="traffic", **kwargs)
    
    def _createDf(self, data, sortasc=False):
        df = pd.DataFrame(data)
        if "start_date" in df.columns:
            df["start_date"] = pd.to_datetime(df["start_date"])
            df.sort_values("start_date", inplace=True, ignore_index=True, ascending=sortasc)
            df["start_day"] = df["start_date"].dt.date
        return df
    
    def formatTrafficEvents(self, idf, update_time, past_hours):
        evsep = "\n\n"
        traffic = idf[idf["evtype"] == "traffic"]
        logging.info(f"Found {len(traffic)} events for update_time {update_time}")
        parts = [
            f"Traffico: ultimo aggiornamento alle {update_time.strftime('%H:%M')}\n"+\
            f"Eventi delle ultime {past_hours} ore da segnalare: {len(traffic)}"
        ]
        for i, row in traffic.iterrows():
            parts.append(f"- {row['start_date']} \n\t{row['desc']}")
        chunks = create_message_chunks(parts, sep=evsep)
        return chunks
    
    def formatClosureEvents(self, closures, update_time, ndays_next):
        try:
            threshold = update_time + timedelta(days=ndays_next)
            df = closures[closures["start_day"] <= threshold.date()]
            grouped = df.groupby("start_day")
            dates = list(grouped.groups)
            evsep = "\n\n"
            parts = [
                f"Chiusure da segnalare nei prossimi {ndays_next} giorni: {len(df)}"
            ]
            for date in dates:
                header = f"Chiusure del {date.strftime('%d/%m/%Y')}"
                i_rows = list(grouped.get_group(date).iterrows())
                parts.append(f"{header}{evsep}- {i_rows[0][1]['desc']}")
                for i, row in i_rows[1:]:
                    parts.append(f"- {row['desc']}")
        except:
            df = closures
            parts = [
                f"Chiusure da segnalare nei prossimi giorni: {len(df)}"
            ]
            for i, row in df.iterrows():
                parts.append(f"- {row['desc']}")
        chunks = create_message_chunks(parts, chunklen=2048)
        return chunks