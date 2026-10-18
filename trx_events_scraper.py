"""
The Exchange TRX - event listing collector (v2)
-----------------------------------------------
Usage:
  pip install requests beautifulsoup4 pandas
  python trx_events_scraper.py live       # snapshot today's events page (run weekly)
  python trx_events_scraper.py wayback    # download archived snapshots (resumable)
  python trx_events_scraper.py discover   # list archived URLs on the whole domain -> data/discovered_urls.csv
  python trx_events_scraper.py articles   # download archived /journal/ + /press-room/ pages -> data/articles.csv
  python trx_events_scraper.py build      # parse data/raw/*_events.html -> data/trx_events.csv

Every command is safe to re-run: files already on disk are skipped, and permanent
failures (404/403/410...) are remembered in data/failures.csv so they aren't retried.
Optional hand-curation: data/organizer_overrides.csv  (columns: pattern,organizer_type,organizer_name)
"""
from __future__ import annotations
import sys, re, time, random, calendar, csv, datetime as dt
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit

import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://www.theexchange.my"
DOMAIN = "theexchange.my"
EVENTS_URL = f"{BASE}/discover/events-and-calendar/"
EXTRA_PAGES = [f"{BASE}/shop/promotions/", f"{BASE}/discover/trx-city-park/"]
WINDOW_FROM = (dt.date.today() - dt.timedelta(days=365 * 2 + 5)).strftime("%Y%m%d")
WINDOW_TO = dt.date.today().strftime("%Y%m%d")

HEADERS = {"User-Agent": "research-scraper (contact: your-email@example.com)"}
DELAY = 4.0              # seconds between requests (+ jitter). archive.org drops connections if you go faster
MAX_ATTEMPTS = 4         # per URL
BACKOFF_CAP = 120        # max seconds between retries of one URL
BREAKER_AFTER = 3        # this many consecutive failed requests => assume we're throttled
COOLDOWN_S = 300         # ...and sleep this long (archive.org blocks usually clear in a few minutes)
MAX_COOLDOWNS = 3        # still failing after this many cooldowns => stop cleanly, rerun later

RAW = Path("data/raw"); RAW.mkdir(parents=True, exist_ok=True)
OUT_CSV = Path("data/trx_events.csv")
FAIL_CSV = Path("data/failures.csv")

# ================================================================= HTTP layer
PERMANENT = {400, 401, 403, 404, 405, 406, 410, 414, 451}   # retrying won't help
RETRYABLE = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}

class RateLimited(Exception):
    """Raised when the server keeps refusing us even after cool-downs."""

@dataclass
class Result:
    ok: bool
    status: int | None = None
    text: str | None = None
    kind: str = "ok"        # ok | permanent | transient
    reason: str = ""

def _retry_after(resp):
    v = resp.headers.get("Retry-After")
    if not v: return None
    try: return min(float(v), 600)
    except ValueError:
        try: return min(max((parsedate_to_datetime(v) - dt.datetime.now(dt.timezone.utc)).total_seconds(), 0), 600)
        except Exception: return None

class Fetcher:
    def __init__(self):
        self.s = requests.Session()           # keep-alive: fewer new connections = less likely to be throttled
        self.s.headers.update(HEADERS)
        self.consec_fail = 0
        self.cooldowns = 0
        self.stats = {"ok": 0, "permanent": 0, "transient": 0}
        self.permanent = set()
        if FAIL_CSV.exists():
            f = pd.read_csv(FAIL_CSV)
            self.permanent = set(f.loc[f["kind"] == "permanent", "url"])

    def _log(self, url, status, kind, reason):
        new = not FAIL_CSV.exists()
        with FAIL_CSV.open("a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            if new: w.writerow(["ts", "url", "status", "kind", "reason"])
            w.writerow([dt.datetime.now().isoformat(timespec="seconds"), url, status, kind, reason[:300]])

    def _cooldown(self):
        self.cooldowns += 1
        if self.cooldowns > MAX_COOLDOWNS:
            raise RateLimited(f"still refused after {MAX_COOLDOWNS} cool-downs")
        print(f"  !! {self.consec_fail} failures in a row - looks like throttling. "
              f"Cooling down {COOLDOWN_S}s ({self.cooldowns}/{MAX_COOLDOWNS})...")
        time.sleep(COOLDOWN_S)
        self.consec_fail = 0

    def _transient(self, attempt, wait=None):
        self.consec_fail += 1
        if self.consec_fail >= BREAKER_AFTER:
            self._cooldown()
        else:
            time.sleep(wait if wait is not None else min(BACKOFF_CAP, 5 * 2 ** (attempt - 1)) + random.uniform(0, 2))

    def get(self, url, params=None, **kw):
        if params is None and url in self.permanent:
            return Result(False, None, None, "permanent", "known permanent failure (data/failures.csv)")
        last = "no attempt"
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                r = self.s.get(url, params=params, timeout=(10, 60), **kw)
            except (requests.ConnectionError, requests.Timeout) as e:
                # e.g. WinError 10061 "actively refused" = archive.org throttling us
                last = f"{type(e).__name__}: {str(e)[:150]}"
                print(f"  net error (attempt {attempt}/{MAX_ATTEMPTS}) {url[-80:]}: {last[:90]}")
                self._transient(attempt); continue
            except requests.RequestException as e:      # bad URL, redirect loop... never going to work
                self.stats["permanent"] += 1
                self._log(url, None, "permanent", f"{type(e).__name__}: {e}")
                return Result(False, None, None, "permanent", str(e))
            code = r.status_code
            if code == 200:
                if len(r.text) < 300 and "html" in r.headers.get("content-type", ""):
                    last = "suspiciously short body"; self._transient(attempt); continue
                self.consec_fail = 0; self.cooldowns = 0; self.stats["ok"] += 1
                return Result(True, 200, r.text)
            if code in PERMANENT:
                self.stats["permanent"] += 1
                if params is None: self.permanent.add(url)
                self._log(url, code, "permanent", r.reason or "")
                print(f"  HTTP {code} (permanent, not retrying): {url[-90:]}")
                return Result(False, code, None, "permanent", r.reason or "")
            if code in RETRYABLE or code >= 500:
                last = f"HTTP {code}"
                ra = _retry_after(r)
                print(f"  HTTP {code} (attempt {attempt}/{MAX_ATTEMPTS})" + (f", server says wait {ra:.0f}s" if ra else ""))
                if code == 429: self.consec_fail = max(self.consec_fail, BREAKER_AFTER - 1)
                self._transient(attempt, wait=ra); continue
            self.stats["permanent"] += 1                 # unexpected 3xx/4xx
            self._log(url, code, "permanent", f"unexpected status {code}")
            return Result(False, code, None, "permanent", f"unexpected status {code}")
        self.stats["transient"] += 1
        self._log(url, None, "transient", last)
        return Result(False, None, None, "transient", last)

    def summary(self):
        print(f"\nrequests: {self.stats['ok']} ok, {self.stats['permanent']} permanent failures, "
              f"{self.stats['transient']} gave up after retries" + (f"  (details: {FAIL_CSV})" if FAIL_CSV.exists() else ""))

FETCH = Fetcher()

def polite():
    time.sleep(DELAY + random.uniform(0, 1.5))

def save_raw(name, html):
    tmp = RAW / (name + ".part")             # write-then-rename so an interrupted run never leaves a half file
    tmp.write_text(html, encoding="utf-8"); tmp.replace(RAW / name)

# ================================================================= live + wayback
def scrape_live():
    stamp = dt.date.today().strftime("%Y%m%d")
    for u in [EVENTS_URL] + EXTRA_PAGES:
        r = FETCH.get(u)
        if r.ok:
            tag = "events" if u == EVENTS_URL else u.rstrip("/").split("/")[-1]
            save_raw(f"live_{stamp}_{tag}.html", r.text)
            print("saved", tag)
        else:
            print(f"FAILED {u}: {r.kind} {r.status or ''} {r.reason}")
        polite()

CDX = "https://web.archive.org/cdx/search/cdx"

def cdx_query(**params):
    """Returns list of dict rows, or None if the request failed (so callers can tell 'empty' from 'error')."""
    r = FETCH.get(CDX, params={**params, "output": "json"})
    if not r.ok: return None
    try:
        rows = __import__("json").loads(r.text)
    except ValueError:
        print("  CDX returned non-JSON (probably an error page):", r.text[:120].replace("\n", " "))
        return None
    if not rows: return []
    head, body = rows[0], rows[1:]
    return [dict(zip(head, row)) for row in body]

def fetch_snapshots(url, tag, collapse):
    rows = cdx_query(url=url, matchType="exact", fl="timestamp,original,digest",
                     filter="statuscode:200", collapse=collapse, **{"from": WINDOW_FROM, "to": WINDOW_TO})
    if rows is None:
        print(f"{tag}: could not list snapshots (see above) - skipping"); return
    print(f"{tag}: {len(rows)} snapshots")
    seen, failed = set(), 0
    for row in rows:
        ts, orig, digest = row["timestamp"], row["original"], row["digest"]
        if digest in seen: continue          # identical content already fetched
        seen.add(digest)
        fn = RAW / f"wb_{ts}_{tag}.html"
        if fn.exists(): continue
        r = FETCH.get(f"https://web.archive.org/web/{ts}id_/{orig}")   # id_ = raw original HTML, no toolbar
        if r.ok:
            save_raw(fn.name, r.text); print("  saved", fn.name)
        else:
            failed += 1
            print(f"  skipped {ts} ({r.kind}{' ' + str(r.status) if r.status else ''}); will retry next run" if r.kind == "transient"
                  else f"  skipped {ts} ({r.status})")
        polite()
    if failed: print(f"{tag}: {failed} snapshots failed - rerun the command to retry the transient ones")

def scrape_wayback():
    fetch_snapshots(EVENTS_URL, "events", collapse="timestamp:8")
    for u in EXTRA_PAGES:
        fetch_snapshots(u, u.rstrip("/").split("/")[-1], collapse="timestamp:6")

# ================================================================= URL discovery
ARTICLE_RE = re.compile(r"^/(journal|about/press-room)/.+", re.I)
EVENTISH_RE = re.compile(r"event|calendar|campaign|promotion|whats-on|news|press|journal|popup|pop-up", re.I)
ASSET_RE = re.compile(r"\.(jpe?g|png|gif|webp|svg|css|js|ico|pdf|woff2?|ttf|mp4|xml|json)$", re.I)

def norm_url(u):
    p = urlsplit(u)
    host = p.hostname.lower().removeprefix("www.")
    path = re.sub(r"/{2,}", "/", p.path or "/")
    if not path.endswith("/") and "." not in path.rsplit("/", 1)[-1]: path += "/"
    return f"https://www.{host}{path}", path

def discover():
    # NOTE v1 bug: it passed matchType=prefix AND a trailing '*' (url=.../journal/*). The '*' became part of the
    # literal prefix, so nothing matched. Here we query the whole domain once and filter locally instead.
    rows = cdx_query(url=DOMAIN, matchType="domain", fl="timestamp,original,mimetype,statuscode",
                     filter="statuscode:200", collapse="urlkey", limit=100000)
    if rows is None: sys.exit("CDX request failed - rerun later (see data/failures.csv)")
    print(f"CDX returned {len(rows)} captures for *.{DOMAIN}")
    if not rows:
        print("empty result. Check by hand in a browser:\n  " +
              f"{CDX}?url={DOMAIN}&matchType=domain&limit=5&output=json"); return
    out = {}
    for r in rows:
        url, path = norm_url(r["original"])
        if ASSET_RE.search(path) or not r["mimetype"].startswith("text/html"): continue
        if url not in out or r["timestamp"] < out[url]["first_seen_ts"]:
            seg = [s for s in path.split("/") if s]
            out[url] = dict(url=url, path=path, first_seen_ts=r["timestamp"], original=r["original"],
                            section="/".join(seg[:2]) if seg else "(home)",
                            is_article=bool(ARTICLE_RE.match(path)), eventish=bool(EVENTISH_RE.search(path)))
    df = pd.DataFrame(out.values()).sort_values(["section", "url"])
    df.to_csv("data/discovered_urls.csv", index=False)
    print(f"{len(df)} unique HTML pages -> data/discovered_urls.csv  ({int(df.is_article.sum())} journal/press articles)")
    print(df["section"].value_counts().head(15).to_string())

def slug(url): return re.sub(r"[^a-z0-9]+", "-", urlsplit(url).path.lower()).strip("-")[:80] or "home"

def articles():
    p = Path("data/discovered_urls.csv")
    if not p.exists(): sys.exit("run `discover` first")
    df = pd.read_csv(p, dtype=str)
    df = df[df.is_article == "True"]
    print(f"{len(df)} articles to fetch")
    for _, r in df.iterrows():
        fn = RAW / f"art_{r.first_seen_ts}_{slug(r.url)}.html"
        if fn.exists(): continue
        res = FETCH.get(f"https://web.archive.org/web/{r.first_seen_ts}id_/{r.original}")
        if res.ok: save_raw(fn.name, res.text); print("  saved", fn.name)
        polite()
    rows = []
    for f in sorted(RAW.glob("art_*.html")):
        s = BeautifulSoup(f.read_text(encoding="utf-8", errors="ignore"), "html.parser")
        meta = lambda k: (s.find("meta", attrs={"property": k}) or s.find("meta", attrs={"name": k}) or {}).get("content")
        t = s.find("time")
        main = s.find(id="mainContent") or s.find("main") or s
        rows.append(dict(file=f.name, title=meta("og:title") or (s.title.string if s.title else None),
                         published=meta("article:published_time") or (t.get("datetime") or t.get_text(strip=True) if t else None),
                         text=main.get_text(" ", strip=True)[:3000]))
    pd.DataFrame(rows).to_csv("data/articles.csv", index=False)
    print(f"{len(rows)} articles -> data/articles.csv")

# ================================================================= dates
MONTHS = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
MONTHS.update({m.lower(): i for i, m in enumerate(calendar.month_abbr) if m})
MONTHS["sept"] = 9
mon = lambda t: MONTHS.get((t or "").lower().rstrip("."))

RANGE = re.compile(r"(\d{1,2})(?:\s*([A-Za-z]{3,9}))?(?:\s*(\d{4}))?\s*(?:–|—|-|to)\s*(\d{1,2})\s*([A-Za-z]{3,9})(?:,?\s*(\d{4}))?", re.I)
UNTIL = re.compile(r"\b(?:until|till|through|ends?(?:\s+on)?)\s+(\d{1,2})\s+([A-Za-z]{3,9})(?:\s+(\d{4}))?", re.I)
FROM = re.compile(r"\bfrom\s+(\d{1,2})\s+([A-Za-z]{3,9})(?:\s+(\d{4}))?", re.I)
US_DATE = re.compile(r"\b([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})")
SINGLE = re.compile(r"\b(\d{1,2})\s+([A-Za-z]{3,9})(?:\s+(\d{4}))?", re.I)
TIME_RE = re.compile(r"\d{1,2}[:.]\d{2}\s*[ap]\.?m\.?\s*(?:–|-|to)\s*\d{1,2}[:.]\d{2}\s*[ap]\.?m\.?", re.I)

def _mkdate(d, mo, y, snap):
    """Explicit year wins. Otherwise pick the year that puts the date closest to the snapshot date
    (fixes '25 December' seen on 2025-01-01 being labelled Dec 2025 instead of Dec 2024)."""
    d = int(d)
    if y:
        try: return dt.date(int(y), mo, d)
        except ValueError: return None
    best = None
    for yy in (snap.year - 1, snap.year, snap.year + 1):
        try: c = dt.date(yy, mo, d)
        except ValueError: continue
        if best is None or abs((c - snap).days) < abs((best - snap).days): best = c
    return best

def _parse_dates_text(text, snap):
    for m in RANGE.finditer(text):
        d1, m1, y1, d2, m2, y2 = m.groups()
        mo2, mo1 = mon(m2), mon(m1) or mon(m2)
        if not mo2: continue
        end = _mkdate(d2, mo2, y2 or y1, snap)
        if not end: continue
        start = _mkdate(d1, mo1, y1 or (y2 if mo1 == mo2 else None), snap) if (y1 or y2) else None
        if start is None:
            try: start = dt.date(end.year if mo1 <= mo2 else end.year - 1, mo1, int(d1))
            except ValueError: continue
        return start, end, m.group(0), "range"
    fm, um = FROM.search(text), UNTIL.search(text)
    fm = fm if fm and mon(fm.group(2)) else None
    um = um if um and mon(um.group(2)) else None
    if fm and um:
        e = _mkdate(um.group(1), mon(um.group(2)), um.group(3), snap)
        s = _mkdate(fm.group(1), mon(fm.group(2)), fm.group(3), snap)
        if s and e: return s, e, f"{fm.group(0)} .. {um.group(0)}", "range"
    if um:
        e = _mkdate(um.group(1), mon(um.group(2)), um.group(3), snap)
        if e: return None, e, um.group(0), "end_only"
    if fm:
        s = _mkdate(fm.group(1), mon(fm.group(2)), fm.group(3), snap)
        if s: return s, None, fm.group(0), "start_only"
    for m in US_DATE.finditer(text):
        if mon(m.group(1)):
            d = _mkdate(m.group(2), mon(m.group(1)), m.group(3), snap)
            if d: return d, d, m.group(0), "single"
    for m in SINGLE.finditer(text):
        if mon(m.group(2)):
            d = _mkdate(m.group(1), mon(m.group(2)), m.group(3), snap)
            if d: return d, d, m.group(0), "single"
    return None, None, None, None

def parse_dates(date_line, text, snap):
    """Try the structured date line first (e.g. '7 – 18 October 2026, 10:00 a.m. – 10:00 p.m.'), then prose."""
    for cand in (date_line, text):
        if cand:
            res = _parse_dates_text(cand, snap)
            if res[3]: return res
    return None, None, None, None

# ================================================================= organizer: mall vs tenant
VENUES = ["Central Exchange", "Raintree Plaza", "TRX City Park", "Market Lane", "Events Lawn", "Urban Node",
          "Sunlight Plaza", "Food Exchange", "City Avenue", "Park Avenue", "Concourse Level", "Concourse"]
VENUE_RE = re.compile("|".join(map(re.escape, VENUES)), re.I)
LOCATIONISH = re.compile(r"\b(level|concourse|plaza|lane|avenue|lawn|node|atrium|floor|park|exchange)\b", re.I)

# Mall-run programmes/campaigns. Matched against the part BEFORE ' | ' (or the whole title if there is none).
MALL_PROGRAM_RE = re.compile(
    r"^(trx\s?eats|christmas|holiday campaign|nye|new year|chinese new year|cny|raya|a luminous raya|mid-?autumn|"
    r"merdeka|deepavali|ramadan|arts\s*&\s*music festival|weeknight cravings|cravings hit|lendlease plus|"
    r"tourist rewards|kimpton rewards|experience the magic of)", re.I)
MALL_TITLE_RE = re.compile(r"\bat the exchange trx\b|\bthe exchange trx\b", re.I)

# Tenants with a store/outlet in the mall. Extend freely, or use data/organizer_overrides.csv.
KNOWN_TENANTS = ["Apple", "Today at Apple", "Aurum", "Aesop", "Beauty Galleria", "Beauty Affairs", "Coach", "diptyque",
                 "Estee Lauder", "Estée Lauder", "Hokkaido Table", "Kissa Diner", "Lancôme", "Lancome", "Lane 23",
                 "Louis Vuitton", "MPH", "Rituals", "SEIBU", "Seibu", "Tiffany & Co.", "Tiffany", "TROVE", "Watsons",
                 "Guerlain", "La Prairie", "Acqua di Parma"]
AMBIGUOUS_WORDS = {"coach", "trove", "rituals", "apple"}   # only match as title prefix, not anywhere in the text

MALL_CUES = re.compile(r"\b(join us|we'?re|we are|we'?ve|we unveil|unveil|throughout the (?:mall|centre|center)|"
                       r"across (?:the )?(?:mall|centre|center)|selected tenants|across your favourite)\b", re.I)
TENANT_CUES = re.compile(r"\b(in-?store|our store|our boutique|boutique|flagship|find (?:us|the (?:pop[- ]?up|store)) on|"
                         r"at (?:the )?\w+ store|counter)\b", re.I)

_overrides = None
def load_overrides():
    global _overrides
    if _overrides is None:
        p = Path("data/organizer_overrides.csv")
        _overrides = pd.read_csv(p).fillna("").to_dict("records") if p.exists() else []
    return _overrides

def _split_title(title):
    """'Brand | Event' or 'Event | Location' -> (prefix, suffix); (None, title) if no pipe."""
    if "|" in title:
        a, b = [x.strip() for x in title.split("|", 1)]
        return a, b
    return None, title.strip()

def _tenant_match(text, prefix_only):
    low = text.lower().strip()
    for t in sorted(KNOWN_TENANTS, key=len, reverse=True):
        tl = t.lower()
        if low == tl or low.startswith(tl + " ") or low.startswith(tl):
            return t
        if not prefix_only and tl not in AMBIGUOUS_WORDS and re.search(rf"(?<!\w){re.escape(tl)}(?!\w)", low):
            return t
    return None

def classify_organizer(title, desc, link, venue):
    """-> (organizer_type, organizer_name, basis). types: mall | tenant | brand_popup | unknown"""
    for o in load_overrides():
        if o["pattern"] and o["pattern"].lower() in title.lower():
            return o["organizer_type"], o["organizer_name"] or None, "manual override"
    prefix, rest = _split_title(title)
    suffix_is_location = prefix is not None and (VENUE_RE.search(rest) or LOCATIONISH.search(rest))
    head = prefix if prefix else title
    is_popup = bool(re.search(r"pop-?\s?up|activation|immersive experience|exclusive experience", f"{title} {desc[:300]}", re.I))
    common = bool(venue)                                   # venue regex only knows shared spaces

    if MALL_PROGRAM_RE.search(head):
        return "mall", "The Exchange TRX", f"mall programme name '{head}'"
    if MALL_TITLE_RE.search(title):
        return "mall", "The Exchange TRX", "title says 'at The Exchange TRX'"
    t = _tenant_match(head, prefix_only=True) or (None if prefix and not suffix_is_location else _tenant_match(title, False))
    if t:
        return "tenant", re.sub(r"^today at ", "", t, flags=re.I), "known tenant in title"
    if link and urlsplit(link).hostname and DOMAIN not in urlsplit(link).hostname and not is_popup:
        return "tenant", None, f"links to external site ({urlsplit(link).hostname})"
    # unknown brand: name inferred from title
    name = re.sub(r"\s*pop-?\s?up.*$", "", head, flags=re.I).strip() or head
    if is_popup and (common or prefix or "pop" in title.lower()):
        return "brand_popup", name, "pop-up/activation by a brand not in tenant list"
    if prefix and not suffix_is_location:
        return "tenant", prefix, "'Brand | Event' title format (brand not in tenant list)"
    m, tn = len(MALL_CUES.findall(desc)), len(TENANT_CUES.findall(desc))
    if m > tn: return "mall", "The Exchange TRX", "weak: mall-voice wording"
    if tn > m: return "tenant", None, "weak: tenant/store wording"
    return "unknown", None, "no signal - add to data/organizer_overrides.csv"

# ================================================================= event type
TYPE_RULES = [
    ("brand pop-up/activation", r"\bpop-?\s?up\b|\bactivation\b|\bsimulator\b|\bcollab"),
    ("festive/seasonal", r"\bchristmas|chinese new year|\bcny\b|\braya\b|hari raya|deepavali|merdeka|new year|\bnye\b|countdown|festive|ramadan|bazaar|mid-autumn|holiday"),
    ("workshop/community", r"\bworkshops?\b|\bclass(es)?\b|\bsessions?\b|masterclass|meet\s*&\s*greet|\byoga\b|\bwellness\b"),
    ("arts/music/performance", r"\bmusic\b|\bconcert\b|\bfestival\b|\bperformances?\b|\bdance\b|\bexhibit"),
    ("retail promotion/campaign", r"\bredeem|spend rm|\bpromotions?\b|\bdeals?\b|\bvoucher|\bsale\b|cravings|\beats\b|gift with purchase|\brewards\b"),
]
def classify(title, desc):
    for text in (title, desc):          # title is a much stronger signal than body text
        for label, pat in TYPE_RULES:
            if re.search(pat, text, re.I): return label
    return "other/unclassified"

# ================================================================= page parsing
def snapshot_meta(path):
    parts = path.stem.split("_")
    return ("wayback" if parts[0] == "wb" else "live"), dt.datetime.strptime(parts[1][:8], "%Y%m%d").date()

_HYPHENS = str.maketrans({"\u2010": "-", "\u2011": "-", "\u2012": "-", "\u00a0": " "})   # 'Pop\u2011Up' defeated the regexes

def _make_row(title, lines, link, section, snap, src):
    title = title.translate(_HYPHENS)
    lines = [l.translate(_HYPHENS) for l in lines if l]
    desc = " ".join(lines)
    date_line = next((l for l in reversed(lines) if len(l) < 120 and re.search(r"\d", l) and
                      any(mon(w) for w in re.findall(r"[A-Za-z]{3,9}", l))), None)
    start, end, raw, prec = parse_dates(date_line, f"{title}. {desc}", snap)
    mv = VENUE_RE.search(f"{date_line or ''} {desc}")
    venue = mv.group(0) if mv else None
    tm = TIME_RE.search(desc)
    otype, oname, basis = classify_organizer(title, desc, link, venue)
    return dict(mall="The Exchange TRX", event_name=title, organizer_type=otype, organizer_name=oname,
                organizer_basis=basis, event_type=classify(title, desc), start_date=start and start.isoformat(),
                end_date=end and end.isoformat(), date_precision=prec, date_text_raw=raw, time_text=tm.group(0) if tm else None,
                venue_in_mall=venue, hosted_in_common_area=bool(venue), listing_section=section, is_dated=bool(start or end),
                description=desc[:1000], link=link, snapshot_date=snap.isoformat(), source=src)

def parse_events_page(html, snap, src):
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find(id="mainContent") or soup.find("main") or soup
    rows, seen, section = [], set(), None
    # pass 1: h2 = section, h3 = event title, following siblings = body (works on archived layouts)
    for h in main.find_all(["h2", "h3"]):
        title = h.get_text(" ", strip=True)
        if h.name == "h2": section = title; continue
        lines, link = [], None
        for sib in h.next_siblings:
            if getattr(sib, "name", None) in ("h2", "h3"): break
            if getattr(sib, "name", None):
                lines += [x for x in sib.get_text("\n", strip=True).split("\n")]
                a = sib.find("a", href=True) or (sib if sib.name == "a" and sib.get("href") else None)
                link = link or (a["href"] if a else None)
        if not lines and h.parent is not None:       # live layout: body text lives inside the same parent as the h3
            lines = [x for x in h.parent.get_text("\n", strip=True).split("\n") if x != title]
            a = h.parent.find("a", href=True); link = a["href"] if a else None
        seen.add(title); rows.append(_make_row(title, lines, link, section, snap, src))
    # pass 2: evergreen cards (<li> with image + text but no h2/h3) - e.g. the Apple / Aurum / MPH blurbs
    for li in main.find_all("li"):
        if li.find(["h2", "h3", "li"]) or not li.find("img"): continue
        t = li.find(["h4", "h5", "h6", "strong", "b"]) or li.find(class_=re.compile("title|heading|name", re.I))
        lines = [x for x in li.get_text("\n", strip=True).split("\n") if x]
        title = t.get_text(" ", strip=True) if t else (lines[0] if lines else "")
        if len(" ".join(lines)) < 40 or not title or title in seen or title.lower().startswith("skip to"): continue
        body = [x for x in lines if x != title and x.upper() not in ("FIND OUT MORE", "BOOK NOW", "LEARN MORE")]
        a = next((a for a in li.find_all("a", href=True) if a["href"].startswith("http")), None)
        h2 = li.find_previous("h2")
        seen.add(title)
        rows.append(_make_row(title, body, a["href"] if a else None, h2.get_text(" ", strip=True) if h2 else None, snap, src))
    return rows

def build():
    rows = []
    for p in sorted(RAW.glob("*_events.html")):
        src, snap = snapshot_meta(p)
        rows += parse_events_page(p.read_text(encoding="utf-8", errors="ignore"), snap, f"{src}:{p.name}")
    if not rows: sys.exit("no event rows parsed - open a file in data/raw and adjust parse_events_page()")
    df = pd.DataFrame(rows)
    key = ["event_name", "start_date", "end_date"]
    first = lambda c: (c, "first"); last = lambda c: (c, "last")
    agg = df.sort_values("snapshot_date").groupby(key, dropna=False).agg(
        mall=first("mall"), organizer_type=last("organizer_type"), organizer_name=last("organizer_name"),
        organizer_basis=last("organizer_basis"), event_type=last("event_type"), date_precision=last("date_precision"),
        date_text_raw=first("date_text_raw"), time_text=last("time_text"), venue_in_mall=last("venue_in_mall"),
        hosted_in_common_area=last("hosted_in_common_area"), listing_section=last("listing_section"),
        is_dated=first("is_dated"), description=last("description"), link=last("link"),
        first_seen=("snapshot_date", "min"), last_seen=("snapshot_date", "max"),
        n_snapshots=("snapshot_date", "count"), source=last("source"),
    ).reset_index()
    agg.to_csv(OUT_CSV, index=False)
    print(f"{len(df)} raw rows -> {len(agg)} unique events -> {OUT_CSV}")
    print(agg["organizer_type"].value_counts().to_string())

COMMANDS = {"live": scrape_live, "wayback": scrape_wayback, "discover": discover, "articles": articles, "build": build}

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd not in COMMANDS:
        print(__doc__); sys.exit(0)
    try:
        COMMANDS[cmd]()
    except RateLimited as e:
        print(f"\nSTOPPED: {e}. Progress is saved - wait 10-15 minutes and rerun the same command to resume.")
        sys.exit(2)
    except KeyboardInterrupt:
        print("\ninterrupted - files saved so far are kept; rerun to resume.")
        sys.exit(130)
    finally:
        FETCH.summary()