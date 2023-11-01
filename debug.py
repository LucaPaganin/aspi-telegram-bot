import json
from pathlib import Path
from automap_fetcher import AutomapFetcher

if __name__ == '__main__':
    af = AutomapFetcher()
    events = af.getTrafficEventsSync("A10")
    closures = af.getClosuresSync("A10")
    Path("/Users/lucapaganin/Downloads/a10.json").write_text(
        json.dumps([e for e in (events+closures)], indent=2)
    )
