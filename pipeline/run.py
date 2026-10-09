"""Personal job alerts: find roles that fit Numan in Lahore, Islamabad, Karachi, Dubai and Riyadh,
remove duplicates, and add new ones to the Google Sheet.

Run:  python -m pipeline.run                 (normal daily run)
      LOOKBACK_DAYS=14 python -m pipeline.run (one-time backfill)
"""
import hashlib
import html
import json
import os
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml

from . import sheets

ROOT = Path(__file__).resolve().parent.parent
CONFIG = yaml.safe_load((ROOT / "config.yaml").read_text())
DATA = ROOT / "data" / "jobs.csv"
LAST_RUN = ROOT / "data" / "last_run.txt"
PKT = timezone(timedelta(hours=5))
TAB = "My jobs"

GROUPS = [(g["name"], re.compile(g["title"], re.I), re.compile(g["requires"], re.I) if g.get("requires") else None)
          for g in CONFIG["role_groups"]]
REMOTE_OK = re.compile(CONFIG.get("remote_open_to") or "$^", re.I)
EXCLUDE = re.compile(CONFIG["exclude_title"], re.I)
CITIES = CONFIG["cities"]
SOURCE_NAMES = {"linkedin": "LinkedIn", "indeed": "Indeed", "bayt": "Bayt", "greenhouse": "Company careers",
                "lever": "Company careers", "workable": "Company careers", "remoteok": "RemoteOK",
                "weworkremotely": "We Work Remotely"}

COLUMNS = ["job_id", "first_seen", "last_seen", "date_posted", "title", "company", "city", "role_group",
           "level", "source", "job_url", "location_raw"]
SHEET_HEADERS = ["Date found", "Posted", "Role", "Company", "City", "Why it matches", "Level", "Found on",
                 "Apply link", "Status"]


# ---------------------------------------------------------------- matching
def role_group(title):
    t = str(title or "")
    if EXCLUDE.search(t):
        return None
    for name, rx, req in GROUPS:
        if rx.search(t) and (req is None or req.search(t)):
            return name
    return None


def _plain(text):
    """Lowercase and drop accents, so LinkedIn's "Karāchi" / "Islāmābād" match "karachi" / "islamabad"."""
    t = unicodedata.normalize("NFKD", str(text or ""))
    return "".join(ch for ch in t if not unicodedata.combining(ch)).lower()


def city_of(location):
    loc = _plain(location)
    for name, keys in CITIES.items():
        if any(k in loc for k in keys):
            return name
    return None


def place(location, is_remote=False):
    """One of the chosen cities, "Remote" (if open to Pakistan / Gulf / worldwide), or None (drop)."""
    loc = str(location or "")
    remote = bool(is_remote) or bool(re.search(r"\b(remote|anywhere|worldwide|work from home|wfh)\b", loc, re.I))
    if remote and CONFIG.get("include_remote"):
        rest = re.sub(r"\b(remote|work from home|wfh|fully|hybrid|-|,|\||/|\(|\))", " ", loc, flags=re.I).strip()
        if not rest or REMOTE_OK.search(loc):
            return "Remote"
        return None  # remote, but only for some other country
    return city_of(loc)


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


def fix_url(url):
    """Indeed's country sites (e.g. ae.indeed.com) return 404 on /viewjob links; the same job ID
    works on www.indeed.com. Pakistan links (pk.indeed.com) work and are left as they are."""
    u = str(url or "")
    return re.sub(r"^https?://(?!pk\.)[a-z]{2}\.indeed\.com/", "https://www.indeed.com/", u)


# ---------------------------------------------------------------- sources
def lookback_days():
    v = os.environ.get("LOOKBACK_DAYS", "").strip()
    return int(v) if v.isdigit() and int(v) > 0 else None


def _proxies():
    raw = os.environ.get("PROXIES", "").strip()
    return [p.strip() for p in raw.split(",") if p.strip()] or None


def jobspy_searches(backfill):
    from jobspy import scrape_jobs
    hours = (backfill or 0) * 24 or CONFIG["hours_old"]
    plan = []
    s = CONFIG["sites"]
    for loc in s["linkedin"]["locations"]:
        for q in s["linkedin"]["queries"]:
            plan.append(("linkedin", q, dict(location=loc, results_wanted=s["linkedin"]["results_per_query"])))
    for country, loc in s["indeed"]["markets"].items():
        for q in s["indeed"]["queries"]:
            plan.append(("indeed", q, dict(location=loc, country_indeed=country,
                                           results_wanted=s["indeed"]["results_per_query"])))
    for loc in s["bayt"]["locations"]:
        for q in s["bayt"]["queries"]:
            plan.append(("bayt", q, dict(location=loc, results_wanted=s["bayt"]["results_per_query"])))

    rows, log, fails = [], [], {}
    for site, q, kw in plan:
        if fails.get(site, 0) >= 3:
            if not any(l.startswith(f"{site}: skipped") for l in log):
                log.append(f"{site}: skipped remaining searches after 3 failures")
            continue
        try:
            df = scrape_jobs(site_name=[site], search_term=q, hours_old=hours, proxies=_proxies(),
                             fetch_description=CONFIG["fetch_description"], verbose=0, **kw)
            for r in df.to_dict("records"):
                rows.append(dict(source=site, title=r.get("title"), company=r.get("company"),
                                 location=r.get("location"), job_url=fix_url(r.get("job_url")),
                                 date_posted=r.get("date_posted"), is_remote=bool(r.get("is_remote") is True)))
            log.append(f"{site} | {kw['location']} | {q[:60]} | {len(df)}")
        except Exception as e:
            fails[site] = fails.get(site, 0) + 1
            log.append(f"{site} | {kw['location']} | {q[:60]} | FAILED: {str(e)[:100]}")
        print(log[-1], flush=True)
        time.sleep(CONFIG["pause_seconds"])
    return rows, log


UA = {"User-Agent": "Mozilla/5.0 (compatible; JobAlertsBot/1.0)"}


def _get(url):
    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=45)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2:
                raise
            time.sleep(3 * (attempt + 1))


def _date(v):
    if v in (None, "", 0):
        return None
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v / 1000 if v > 1e11 else v, tz=timezone.utc).date().isoformat()
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


def career_pages():
    rows, log = [], []
    cp = CONFIG.get("career_pages") or {}
    for slug, name in (cp.get("greenhouse") or {}).items():
        try:
            data = _get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs") or {}
            got = [dict(source="greenhouse", title=j.get("title"), company=j.get("company_name") or name,
                        location=" | ".join(str(x) for x in [(j.get("location") or {}).get("name")] +
                                            [o.get("name") for o in j.get("offices") or []] if x),
                        job_url=j.get("absolute_url"), date_posted=_date(j.get("first_published") or j.get("updated_at")))
                   for j in data.get("jobs", [])]
            rows += got
            log.append(f"greenhouse | {name} | {len(got)}")
        except Exception as e:
            log.append(f"greenhouse | {name} | FAILED: {str(e)[:100]}")
    for slug, name in (cp.get("lever") or {}).items():
        try:
            data = _get(f"https://api.lever.co/v0/postings/{slug}?mode=json") or []
            got = []
            for j in data:
                c = j.get("categories") or {}
                got.append(dict(source="lever", title=j.get("text"), company=name,
                                location=" | ".join(str(x) for x in [c.get("location")] + list(c.get("allLocations") or []) if x),
                                job_url=j.get("hostedUrl"), date_posted=_date(j.get("createdAt")),
                                is_remote=j.get("workplaceType") == "remote"))
            rows += got
            log.append(f"lever | {name} | {len(got)}")
        except Exception as e:
            log.append(f"lever | {name} | FAILED: {str(e)[:100]}")
    for slug, name in (cp.get("workable") or {}).items():
        try:
            data = _get(f"https://apply.workable.com/api/v1/widget/accounts/{slug}") or {}
            got = []
            for j in data.get("jobs", []):
                locs = [", ".join(x for x in (j.get("city"), j.get("state"), j.get("country")) if x)]
                locs += [", ".join(x for x in (l.get("city"), l.get("region"), l.get("country")) if x)
                         for l in j.get("locations") or []]
                got.append(dict(source="workable", title=j.get("title"), company=data.get("name") or name,
                                location=" | ".join(l for l in locs if l), job_url=j.get("url"),
                                date_posted=_date(j.get("published_on") or j.get("created_at")),
                                is_remote=bool(j.get("telecommuting"))))
            rows += got
            log.append(f"workable | {name} | {len(got)}")
        except Exception as e:
            log.append(f"workable | {name} | FAILED: {str(e)[:100]}")
    for l in log:
        print(l, flush=True)
    return rows, log


def remote_boards():
    """RemoteOK + We Work Remotely: only jobs open worldwide / Asia / Pakistan / Gulf."""
    import xml.etree.ElementTree as ET
    rows, log = [], []
    wanted = CONFIG.get("remote_boards") or []
    if "remoteok" in wanted:
        try:
            r = requests.get("https://remoteok.com/api", headers=UA, timeout=45)
            r.raise_for_status()
            r.encoding = "utf-8"
            got = [dict(source="remoteok", title=j.get("position"), company=j.get("company"),
                        location=(j.get("location") or "Remote") + " | Remote", job_url=j.get("url"),
                        date_posted=_date(j.get("date") or j.get("epoch")), is_remote=True)
                   for j in r.json() if isinstance(j, dict) and "position" in j
                   and REMOTE_OK.search(j.get("location") or "")]
            rows += got
            log.append(f"remoteok | open to Pakistan/Gulf/worldwide | {len(got)}")
        except Exception as e:
            log.append(f"remoteok | FAILED: {str(e)[:100]}")
    if "weworkremotely" in wanted:
        try:
            r = requests.get("https://weworkremotely.com/remote-jobs.rss", headers=UA, timeout=45)
            r.raise_for_status()
            got = []
            for it in ET.fromstring(r.content).iter("item"):
                g = lambda tag: (it.findtext(tag) or "").strip()
                region, country = g("region"), g("country")
                if not REMOTE_OK.search(f"{region} {country}"):
                    continue
                company, _, title = g("title").partition(": ")
                if not title:
                    company, title = "", g("title")
                try:
                    posted = datetime.strptime(g("pubDate"), "%a, %d %b %Y %H:%M:%S %z").date().isoformat()
                except ValueError:
                    posted = None
                got.append(dict(source="weworkremotely", title=html.unescape(title), company=html.unescape(company),
                                location=f"{region} | Remote", job_url=g("link"), date_posted=posted, is_remote=True))
            rows += got
            log.append(f"weworkremotely | open to Pakistan/Gulf/worldwide | {len(got)}")
        except Exception as e:
            log.append(f"weworkremotely | FAILED: {str(e)[:100]}")
    for l in log:
        print(l, flush=True)
    return rows, log


# ---------------------------------------------------------------- main
def main():
    today = datetime.now(PKT).date()
    backfill = lookback_days()
    print(f"Mode: {'BACKFILL ' + str(backfill) + ' days' if backfill else 'daily'}", flush=True)

    js_rows, log = jobspy_searches(backfill)
    cp_rows, cp_log = career_pages()
    rb_rows, rb_log = remote_boards()
    log += cp_log + rb_log
    window_lo = (today - timedelta(days=backfill or CONFIG["feed_window_days"])).isoformat()
    cp_rows = [r for r in cp_rows + rb_rows if not r["date_posted"] or r["date_posted"] >= window_lo]

    raw = pd.DataFrame(js_rows + cp_rows)
    fresh = pd.DataFrame(columns=COLUMNS)
    if len(raw):
        raw = raw.fillna("")
        raw["title"] = raw["title"].astype(str).map(html.unescape).str.strip()
        raw["company"] = raw["company"].astype(str).str.strip()
        raw["role_group"] = raw["title"].map(role_group)
        if "is_remote" not in raw:
            raw["is_remote"] = False
        raw["city"] = [place(l, r is True) for l, r in zip(raw["location"], raw["is_remote"])]
        dropped = raw[raw["role_group"].notna() & raw["city"].isna()]
        log.append("Matching titles dropped for location (top 15): "
                   + json.dumps(dropped["location"].astype(str).value_counts().head(15).to_dict(), ensure_ascii=False))
        m = raw[raw["role_group"].notna() & raw["city"].notna() & (raw["job_url"].astype(str) != "")
                & (raw["company"] != "")].copy()
        m["level"] = m["title"].map(level)
        m["job_id"] = [job_id(c, t, ci) for c, t, ci in zip(m["company"], m["title"], m["city"])]
        m["date_posted"] = pd.to_datetime(m["date_posted"], errors="coerce").dt.date.astype(str).replace({"NaT": "", "None": "", "nan": ""})
        m["location_raw"] = m["location"].astype(str)
        fresh = m.drop_duplicates("job_url").drop_duplicates("job_id")

    hist = pd.read_csv(DATA, dtype=str, keep_default_na=False) if DATA.exists() else pd.DataFrame(columns=COLUMNS)
    if len(hist):
        hist["job_url"] = hist["job_url"].map(fix_url)
    t = today.isoformat()
    win = (today - timedelta(days=CONFIG["repost_window_days"])).isoformat()
    recent = hist[hist["last_seen"] >= win]
    seen = fresh["job_id"].isin(set(recent["job_id"])) | fresh["job_url"].isin(set(recent["job_url"]))
    new = fresh[~seen].copy()
    new["first_seen"], new["last_seen"] = t, t
    if backfill:  # backfilled jobs are dated by when they were posted
        new["first_seen"] = [p if p and window_lo <= p <= t else t for p in new["date_posted"]]
    hist.loc[hist["job_id"].isin(set(fresh.loc[seen, "job_id"])), "last_seen"] = t
    hist = pd.concat([hist, new.reindex(columns=COLUMNS)], ignore_index=True).fillna("")
    DATA.parent.mkdir(exist_ok=True)
    hist.reindex(columns=COLUMNS).to_csv(DATA, index=False)

    summary = (f"raw={len(raw)}  matching={len(fresh)}  new={len(new)}  total={len(hist)}\n"
               f"New by city: {new['city'].value_counts().to_dict() if len(new) else {}}\n"
               f"New by role group: {new['role_group'].value_counts().to_dict() if len(new) else {}}")
    print("\n" + summary)
    LAST_RUN.write_text(f"Run at {datetime.now(PKT):%Y-%m-%d %H:%M} PKT "
                        f"({'backfill ' + str(backfill) + ' days' if backfill else 'daily'})\n"
                        + summary + "\n\n" + "\n".join(log) + "\n")

    def sheet_rows(df):
        df = df.sort_values(["first_seen", "date_posted"], ascending=False)
        return [[r.first_seen, r.date_posted, r.title, r.company, r.city, r.role_group, r.level,
                 SOURCE_NAMES.get(r.source, r.source), r.job_url, ""] for r in df.itertuples()]

    if backfill:
        # first fill: write the whole list, newest first
        sheets.write("master", TAB, SHEET_HEADERS, sheet_rows(hist), mode="replace",
                     banner="My job alerts | Lahore, Islamabad, Karachi, Dubai, Riyadh + Remote | updated daily at 7 AM PKT",
                     first=True)
    elif len(new):
        # daily: add only new jobs below the last used row. format=False means the tab is not
        # restyled at all, so existing rows (and anything you typed in them) are never touched.
        sheets.write("master", TAB, SHEET_HEADERS, sheet_rows(new), format=False)
    sheets.write("master", "runs_log", ["run_at_pkt", "mode", "found", "matching", "new", "failed_searches"],
                 [[f"{datetime.now(PKT):%Y-%m-%d %H:%M}", "backfill" if backfill else "daily", len(raw),
                   len(fresh), len(new), sum("FAILED" in l for l in log)]])
    if len(raw) == 0:
        raise SystemExit("No jobs found at all: every source failed. Check data/last_run.txt.")


if __name__ == "__main__":
    main()
