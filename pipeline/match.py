"""Matching rules shared by the daily run and the discovery workflows.

- role_group(title): which of your role groups a title fits (or None)
- Geo.place(location, is_remote): (city, country) for your markets, ("Remote", None), or (None, None)
- visa / nationals checks on job descriptions
- government visa-sponsor lists (UK, Netherlands, Canada)
"""
import hashlib
import re
import unicodedata
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIG = yaml.safe_load((ROOT / "config.yaml").read_text())
SPONSOR_DIR = ROOT / "data" / "sponsors"

_rx = lambda p: re.compile(p or r"$^", re.I)
GROUPS = [(g["name"], _rx(g["title"]), _rx(g["requires"]) if g.get("requires") else None)
          for g in CONFIG["role_groups"]]
EXCLUDE = _rx(CONFIG["exclude_title"])
REMOTE_OK = _rx(CONFIG.get("remote_open_to"))
NAT_TITLE = _rx(CONFIG.get("nationals_title"))
NAT_TEXT = _rx(CONFIG.get("nationals_text"))
VISA_YES = _rx(CONFIG.get("visa_yes"))
VISA_NO = _rx(CONFIG.get("visa_no"))
US_MARK = re.compile(r"united states|\busa\b|\bu\.s\.a?\b", re.I)


def plain(text):
    """Lowercase and drop accents, so "Karāchi" / "München" match "karachi" / "munchen"."""
    t = unicodedata.normalize("NFKD", str(text or ""))
    return "".join(ch for ch in t if not unicodedata.combining(ch)).lower()


def role_group(title):
    t = str(title or "")
    if EXCLUDE.search(t):
        return None
    for name, rx, req in GROUPS:
        if rx.search(t) and (req is None or req.search(t)):
            return name
    return None


def level(title):
    t = str(title or "").lower()
    for name, rx in (("Leadership", r"\b(head|director|vp|vice president|chief)\b"),
                     ("Lead / Manager", r"\b(lead|manager|principal|staff)\b"),
                     ("Senior", r"\b(senior|sr\.?)\b")):
        if re.search(rx, t):
            return name
    return "Mid / unspecified"


def norm(s):
    s = re.sub(r"\b(pvt|private|ltd|limited|llc|inc|co|company)\b\.?", " ", str(s or "").lower())
    s = re.sub(r"[\(\[].*?[\)\]]", " ", s)
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s)).strip()


def job_id(company, title, city):
    return hashlib.sha1(f"{norm(company)}|{norm(title)}|{city}".encode()).hexdigest()[:12]


INDEED_404 = re.compile(r"^https?://(ae|sa|qa|bh|om|kw|eg)\.indeed\.com/")


def fix_url(url):
    """Indeed's Gulf / Egypt sites return 404 on /viewjob links; the same job ID works on www.indeed.com."""
    return INDEED_404.sub("https://www.indeed.com/", str(url or ""))


# ---------------------------------------------------------------- places
def _word_rx(words):
    latin = [plain(w) for w in words if re.search(r"[a-z]", plain(w))]
    other = [w for w in words if not re.search(r"[a-z]", plain(w))]
    rx = re.compile(r"(?<![a-z])(" + "|".join(re.escape(w) for w in latin) + r")(?![a-z])") if latin else None
    return rx, other


def _hit(rx_other, text_plain, text_raw):
    rx, other = rx_other
    return bool((rx and rx.search(text_plain)) or any(o in text_raw for o in other))


class Geo:
    GENERIC = re.compile(r"\b(remote|hybrid|on[- ]?site|onsite|in[- ]office|office|province|region|state|governorate|"
                         r"emirate|emirates|metropolitan|metro|area|greater|kingdom of|republic of|the|of|and|or|"
                         r"multiple locations|various|locations?|city|\d+)\b", re.I)

    def __init__(self, config):
        self.markets = config["markets"]
        self.not_if = {k: re.compile(v, re.I) for k, v in (config.get("city_not_if") or {}).items()}
        self.cities = []      # (city, country, matcher)
        self.countries = []   # (country, matcher)
        for country, m in self.markets.items():
            for city, words in (m.get("cities") or {}).items():
                self.cities.append((city, country, _word_rx(words)))
            self.countries.append((country, _word_rx(m.get("names") or [country])))
        self.city_country = {c: k for c, k, _ in self.cities}
        self.city_words = [w for m in self.markets.values() for ws in (m.get("cities") or {}).values() for w in ws]

    def region(self, city, country=None):
        if city == "Remote":
            return "Remote"
        country = country or self.city_country.get(city) or (city.split(" (")[0] if city else None)
        m = self.markets.get(country) or {}
        r = m.get("region", "")
        return r if r == country or r == "Pakistan" else f"{r}: {country}" if r else country or ""

    def visa_rule(self, country):
        return (self.markets.get(country) or {}).get("visa", "gulf")

    def sponsor_list(self, country):
        return (self.markets.get(country) or {}).get("sponsor_list")

    def place(self, location, is_remote=False):
        """(city, country) in one of your markets, ("Remote", None), or (None, None) to drop."""
        raw = str(location or "")
        loc = plain(raw)
        remote = bool(is_remote) or bool(re.search(r"\b(remote|anywhere|worldwide|work from home|wfh)\b", loc))
        if remote and CONFIG.get("include_remote"):
            rest = re.sub(r"\b(remote|work from home|wfh|fully|hybrid|anywhere)\b|[-,|/()]", " ", loc).strip()
            if not rest or REMOTE_OK.search(loc):
                return "Remote", None
            # remote but tied to a place: keep only if that place is one of your cities
        for city, country, matcher in self.cities:
            if _hit(matcher, loc, raw):
                bad = self.not_if.get(city) or self.not_if.get(country)
                if (bad and bad.search(loc)) or US_MARK.search(loc):
                    continue
                return city, country
        for country, matcher in self.countries:
            if _hit(matcher, loc, raw):
                bad = self.not_if.get(country)
                if bad and bad.search(loc):
                    continue
                # only the country is given (no other town named): keep it
                rest = loc
                rx, other = matcher
                rest = rx.sub(" ", rest) if rx else rest
                for o in other:
                    rest = rest.replace(o, " ")
                rest = self.GENERIC.sub(" ", rest)
                rest = re.sub(r"[^a-z؀-ۿ]+", " ", rest).strip()
                if not rest:
                    return f"{country} (city not stated)", country
        return None, None


GEO = Geo(CONFIG)


# ---------------------------------------------------------------- description checks
def description_from_html(text):
    from html import unescape
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", str(text or ""))
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(text)).strip()


def nationals_only(title, text=""):
    return bool(NAT_TITLE.search(str(title or "")) or (text and NAT_TEXT.search(str(text))))


def visa_says(text):
    """'no' (ad rules out sponsorship / needs local right to work), 'yes', or '' (says nothing)."""
    t = re.sub(r"\s+", " ", str(text or ""))
    if not t:
        return ""
    if VISA_NO.search(t):
        return "no"
    if VISA_YES.search(t):
        return "yes"
    return ""


def sponsor_norm(s):
    s = plain(s)
    s = re.sub(r"\b(pvt|private|ltd|limited|llc|llp|plc|inc|co|company|corp|corporation|gmbh|b\.?v\.?|n\.?v\.?|"
               r"uk|group|holdings?|international|t/a.*)\b\.?", " ", s)
    s = re.sub(r"[\(\[].*?[\)\]]", " ", s)
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s)).strip()


class Sponsors:
    """Government lists of employers licensed to sponsor work visas (refreshed weekly)."""

    def __init__(self):
        self.lists = {}

    def _load(self, key):
        if key not in self.lists:
            f = SPONSOR_DIR / f"{key}.txt"
            names = [l.strip() for l in f.read_text().splitlines() if l.strip()] if f.exists() else []
            self.lists[key] = (set(names), names)
        return self.lists[key]

    def has(self, key, company):
        if not key:
            return False
        exact, names = self._load(key)
        n = sponsor_norm(company)
        if not n or not names:
            return False
        if n in exact:
            return True
        if len(n) < 5:
            return False
        from rapidfuzz import fuzz, process
        hit = process.extractOne(n, names, scorer=fuzz.ratio, score_cutoff=94)
        return bool(hit)


SPONSORS = Sponsors()
