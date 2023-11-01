from datetime import datetime
import json, re

MONTHMAP_ITA_ENG = {
    "gennaio": "january",
    "febbraio": "february",
    "marzo": "march",
    "aprile": "april",
    "maggio": "may",
    "giugno": "june",
    "luglio": "july",
    "agosto": "august",
    "settembre": "september",
    "ottobre": "october",
    "novembre": "november",
    "dicembre": "december",
}
MONTHS = list(MONTHMAP_ITA_ENG)
RGX_ROAD_NAME = re.compile(r"^([aA]\d+).*$")

def validateRoadName(road_name):
    m = RGX_ROAD_NAME.match(road_name)
    if not m:
        raise ValueError(f"Invalid road name {road_name}")
    return m.groups()[0].upper()

class RoadEvent(object):
    def __init__(self, start_date, road_name, desc, evtype, end_date=None, datefmt=None, **kwargs):
        self.start_date = start_date
        self.road_name = road_name
        self.evtype = evtype
        self.desc = desc
        self.end_date = end_date
        self._datefmt = datefmt if datefmt is not None else "%d %B %Y %H:%M"
        self._add_data = kwargs
        self._validateAttrs()
    
    def _validateAttrs(self):
        if isinstance(self.start_date, str):
            self.start_date = self._parseDate(self.start_date)
        if isinstance(self.end_date, str):
            self.end_date = self._parseDate(self.end_date)
        self.road_name = validateRoadName(self.road_name)
        
    
    def _parseDate(self, datestr):
        res = datetime.strptime(datestr, self._datefmt)
        return res
    
    def to_dict(self):
        return {
            "start_date": self.start_date.isoformat() if isinstance(self.start_date, datetime) else self.start_date,
            "end_date": self.end_date.isoformat() if isinstance(self.end_date, datetime) else self.end_date,
            "desc": self.desc,
            "road_name": self.road_name,
            "evtype": self.evtype,
            **self._add_data
        }
    
    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)
    
    def __str__(self):
        return self.__repr__()

def format_events_df(df):
    pass