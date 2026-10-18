"""
The Exchange TRX - event listing collector
------------------------------------------
Sources:
  1. Live site  : https://www.theexchange.my/discover/events-and-calendar/
  2. Wayback    : archived snapshots of the same page (and other pages, saved raw)

Usage:
  pip install requests beautifulsoup4 pandas python-dateutil
  python trx_events_scraper.py live               # scrape today's page (run weekly to build your own archive)
  python trx_events_scraper.py wayback            # list + download + parse snapshots, last ~2 years
  python trx_events_scraper.py discover           # list archived URLs under /journal/ and /about/press-room/
  python trx_events_scraper.py build              # parse everything in data/raw -> data/trx_events.csv

NOTE: written without being able to test against the live site's raw HTML.
Run `live` first and check data/raw/ + the CSV; adjust parse_events_page() if needed.
"""
import sys, re, time, json, hashlib, calendar, datetime as dt
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://www.theexchange.my"
EVENTS_URL = f"{BASE}/discover/events-and-calendar/"
EXTRA_PAGES = [  # saved raw for later parsing; same snapshot approach
    f"{BASE}/shop/promotions/",
    f"{BASE}/discover/trx-city-park/",
]
WINDOW_FROM = (dt.date.today() - dt.timedelta(days=365 * 2 + 5)).strftime("%Y%m%d")
WINDOW_TO = dt.date.today().strftime("%Y%m%d")

HEADERS = {"User-Agent": "research-scraper (contact: your-email@example.com)"}
DELAY = 2.0  # seconds between requests - be polite, archive.org rate-limits

RAW = Path("data/raw"); RAW.mkdir(parents=True, exist_ok=True)
OUT_CSV = Path("data/trx_events.csv")

# ----------------------------------------------------------------- fetching
def get(url, **kw):
    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60, **kw)
            if r.status_code == 429:
                time.sleep(30 * (attempt + 1)); continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            print(f"  retry {attempt+1} {url}: {e}")
            time.sleep(5 * (attempt + 1))
    return None

def save_raw(name, html):
    (RAW / name).write_text(html, encoding="utf-8")

def scrape_live():
    r = get(EVENTS_URL)
    if not r: sys.exit("live fetch failed")
    stamp = dt.date.today().strftime("%Y%m%d")
    save_raw(f"live_{stamp}_events.html", r.text)
    for u in EXTRA_PAGES:
        time.sleep(DELAY)
        rr = get(u)
        if rr:
            save_raw(f"live_{stamp}_{u.rstrip('/').split('/')[-1]}.html", rr.text)
    print("saved live snapshot", stamp)

# ----------------------------------------------------------------- wayback
CDX = "https://web.archive.org/cdx/search/cdx"

def cdx_list(url, match="exact", collapse="timestamp:6"):
    """collapse=timestamp:6 -> max one snapshot per month; use timestamp:8 for per-day."""
    params = dict(url=url, matchType=match, output="json", from_=WINDOW_FROM, to=WINDOW_TO,
                  fl="timestamp,original,statuscode,digest", filter="statuscode:200",
                  collapse=collapse)
    params["from"] = params.pop("from_")
    r = get(CDX, params=params)
    if not r: return []
    rows = r.json()
    return rows[1:] if rows else []

def fetch_snapshots(url, tag, collapse):
    rows = cdx_list(url, collapse=collapse)
    print(f"{tag}: {len(rows)} snapshots")
    seen = set()
    for ts, orig, _, digest in rows:
        if digest in seen:  # identical content to one already fetched
            continue
        seen.add(digest)
        fn = RAW / f"wb_{ts}_{tag}.html"
        if fn.exists(): continue
        # "id_" returns the original HTML without the Wayback toolbar/rewritten links
        snap = f"https://web.archive.org/web/{ts}id_/{orig}"
        r = get(snap)
        if r:
            fn.write_text(r.text, encoding="utf-8")
            print("  saved", fn.name)
        time.sleep(DELAY)

def scrape_wayback():
    # events page: weekly-ish granularity because listings change often
    fetch_snapshots(EVENTS_URL, "events", collapse="timestamp:8")
    for u in EXTRA_PAGES:
        fetch_snapshots(u, u.rstrip("/").split("/")[-1], collapse="timestamp:6")

def discover():
    out = []
    for prefix in (f"{BASE}/journal/*", f"{BASE}/about/press-room/*"):
        rows = cdx_list(prefix, match="prefix", collapse="urlkey")
        out += [(r[1], r[0]) for r in rows]
    pd.DataFrame(out, columns=["url", "first_seen_ts"]).drop_duplicates("url") \
      .to_csv("data/discovered_urls.csv", index=False)
    print(f"{len(out)} archived article/press URLs -> data/discovered_urls.csv")

# ----------------------------------------------------------------- date parsing
MONTHS = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
MONTHS.update({m.lower(): i for i, m in enumerate(calendar.month_abbr) if m})

RANGE = re.compile(
    r"(\d{1,2})\s*([A-Za-z]{3,9})?\s*(\d{4})?\s*(?:–|—|-|to|until)\s*(\d{1,2})\s*([A-Za-z]{3,9})\s*(\d{4})?",
    re.I)
SINGLE = re.compile(r"(\d{1,2})\s+([A-Za-z]{3,9})\s*(\d{4})?", re.I)
US_DATE = re.compile(r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})")   # "December 31, 2025"

def parse_dates(text, ref_year):
    """Return (start_date, end_date, raw_match) as ISO strings or (None, None, None)."""
    m = RANGE.search(text)
    if m:
        d1, m1, y1, d2, m2, y2 = m.groups()
        mo2 = MONTHS.get((m2 or "").lower())
        mo1 = MONTHS.get((m1 or "").lower()) or mo2
        if mo2 and mo1:
            ye = int(y2 or y1 or ref_year)
            ys = int(y1 or ye)
            if not (y1 or y2) and mo1 > mo2:   # e.g. 20 Dec - 3 Jan, no year given
                ys = ye - 1
            try:
                return (dt.date(ys, mo1, int(d1)).isoformat(),
                        dt.date(ye, mo2, int(d2)).isoformat(), m.group(0))
            except ValueError:
                pass
    m = US_DATE.search(text)
    if m and MONTHS.get(m.group(1).lower()):
        try:
            dd = dt.date(int(m.group(3)), MONTHS[m.group(1).lower()], int(m.group(2))).isoformat()
            return dd, dd, m.group(0)
        except ValueError:
            pass
    for m in SINGLE.finditer(text):
        d, mo, y = m.groups()
        mo = MONTHS.get(mo.lower())
        if mo:
            try:
                dd = dt.date(int(y or ref_year), mo, int(d)).isoformat()
                return dd, dd, m.group(0)
            except ValueError:
                continue
    return None, None, None

# ----------------------------------------------------------------- classification
TYPE_RULES = [
    ("festive/seasonal", r"christmas|chinese new year|cny|raya|hari raya|deepavali|merdeka|new year|countdown|festive|ramadan|bazaar"),
    ("brand pop-up/activation", r"pop-?up|activation|experience|launch|simulator|collab"),
    ("retail promotion/campaign", r"redeem|spend rm|promotion|deals|voucher|sale|cravings|eats|gift with purchase|rewards"),
    ("arts/music/performance", r"music|concert|festival|performance|live |dance|art |exhibit|show"),
    ("workshop/community", r"workshop|class|session|community|yoga|run|sports|wellness|kids"),
]
def classify(text):
    t = text.lower()
    for label, pat in TYPE_RULES:
        if re.search(pat, t):
            return label
    return "other/unclassified"

# ----------------------------------------------------------------- parsing
def snapshot_meta(path):
    """-> (source, snapshot_date) from filename like wb_20250312123456_events.html / live_20261018_events.html"""
    parts = path.stem.split("_")
    src = "wayback" if parts[0] == "wb" else "live"
    ts = parts[1]
    return src, dt.datetime.strptime(ts[:8], "%Y%m%d").date()

def parse_events_page(html, snap_date, src_url):
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find(id="mainContent") or soup.find("main") or soup
    rows, section = [], None
    # Observed layout: h2 "Must-see events" / "More Events at ..." then h3 per event + text
    for h in main.find_all(["h2", "h3"]):
        title = h.get_text(" ", strip=True)
        if h.name == "h2":
            section = title
            continue
        body, link = [], None
        for sib in h.next_siblings:
            if getattr(sib, "name", None) in ("h2", "h3"):
                break
            if getattr(sib, "name", None):  # a Tag (skip bare text nodes)
                body.append(sib.get_text(" ", strip=True))
                a = sib.find("a", href=True)
                link = link or (a["href"] if a else None)
        desc = " ".join(b for b in body if b)
        start, end, raw_date = parse_dates(f"{title}. {desc}", snap_date.year)
        # venue/time line, e.g. "10:00 a.m. – 10:00 p.m. Central Exchange, The Exchange TRX"
        venue = None
        mv = re.search(r"(Central Exchange|Raintree Plaza|TRX City Park|Market Lane|Events Lawn|Urban Node|"
                       r"Sunlight Plaza|Food Exchange|City Avenue|Park Avenue)", desc, re.I)
        if mv: venue = mv.group(1)
        rows.append(dict(
            mall="The Exchange TRX",
            event_name=title,
            event_type=classify(f"{title} {desc}"),
            start_date=start, end_date=end, date_text_raw=raw_date,
            venue_in_mall=venue,
            listing_section=section,
            is_dated=bool(start),          # False => evergreen tenant blurbs (Apple, Aurum, MPH...)
            description=desc[:1000],
            link=link,
            snapshot_date=snap_date.isoformat(),
            source=src_url,
        ))
    return rows

def build():
    rows = []
    for p in sorted(RAW.glob("*_events.html")):
        src, snap = snapshot_meta(p)
        rows += parse_events_page(p.read_text(encoding="utf-8", errors="ignore"), snap, f"{src}:{p.name}")
    if not rows:
        sys.exit("no event rows parsed - open a file in data/raw and adjust parse_events_page()")
    df = pd.DataFrame(rows)
    # same event appears in many snapshots -> keep one row, remember first/last time we saw it
    key = ["event_name", "start_date", "end_date"]
    agg = df.sort_values("snapshot_date").groupby(key, dropna=False).agg(
        mall=("mall", "first"), event_type=("event_type", "first"),
        venue_in_mall=("venue_in_mall", "first"), date_text_raw=("date_text_raw", "first"),
        is_dated=("is_dated", "first"), description=("description", "last"), link=("link", "last"),
        first_seen=("snapshot_date", "min"), last_seen=("snapshot_date", "max"),
        n_snapshots=("snapshot_date", "count"), source=("source", "last"),
    ).reset_index()
    agg.to_csv(OUT_CSV, index=False)
    print(f"{len(df)} raw rows -> {len(agg)} unique events -> {OUT_CSV}")

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    {"live": scrape_live, "wayback": scrape_wayback, "discover": discover, "build": build}.get(
        cmd, lambda: print(__doc__))()