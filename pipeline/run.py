"""Personal job alerts: find roles that fit Numan across all his markets, check visa / nationals
rules, remove duplicates, and add new ones to the Google Sheet (append only).

The daily workflow runs in two steps:
  python -m pipeline.run collect --shard K --shards N   (N runners in parallel, each a share of the searches)
  python -m pipeline.run merge                          (combine, dedupe, write the Sheet and history)
Locally, `python -m pipeline.run` does both in one process.

LOOKBACK_DAYS=7 looks further back once. It only ever ADDS rows; existing Sheet rows are never touched.
"""
import argparse
import csv
import html
import json
import logging
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from . import extra, more, sheets
from .match import (CONFIG, GEO, SPONSORS, fix_url, job_id, level, nationals_only, norm, role_group,
                    visa_says)

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
JOBS = DATA / "jobs.csv"
LAST_RUN = DATA / "last_run.txt"
SITES = DATA / "sites.csv"
SITE_STATE = DATA / "site_state.csv"
SEEN_LINKS = DATA / "seen_links.csv"
SCREENED = DATA / "screened.csv"
OUT = ROOT / "out"
PKT = timezone(timedelta(hours=5))
TAB = "My jobs"

SOURCE_NAMES = {"linkedin": "LinkedIn", "indeed": "Indeed", "bayt": "Bayt", "remoteok": "RemoteOK",
                "weworkremotely": "We Work Remotely", "rozee": "Rozee.pk", "gulftalent": "GulfTalent",
                "careerjet": "Careerjet", "jooble": "Jooble", "google_jobs": "Google Jobs", "remotive": "Remotive",
                "himalayas": "Himalayas", "jobicy": "Jobicy", "workingnomads": "Working Nomads",
                "arbeitnow": "Arbeitnow", "telegram": "Telegram", "google_alerts": "Google Alerts",
                "brave": "Web search (Brave)", "university": "University website", "site": "Company website"}
for _p in ("greenhouse", "lever", "workable", "ashby", "smartrecruiters", "recruitee", "bamboohr", "personio",
           "workday", "breezy", "teamtailor", "pinpoint"):
    SOURCE_NAMES[_p] = "Company careers"
JOBSPY_SITES = {"linkedin", "indeed", "bayt"}
LONG_WINDOW = {"rozee", "gulftalent", "site", "university"}   # sources read from sitemaps / pages

COLUMNS = ["job_id", "first_seen", "last_seen", "date_posted", "title", "company", "city", "role_group",
           "level", "source", "job_url", "location_raw", "region", "note"]
SHEET_HEADERS = ["Date found", "Posted", "Role", "Company", "City", "Why it matches", "Level", "Found on",
                 "Apply link", "Status", "Region"]


def today_pkt():
    return datetime.now(PKT).date()


def lookback_days():
    v = os.environ.get("LOOKBACK_DAYS", "").strip()
    return int(v) if v.isdigit() and int(v) > 0 else None


def _proxies():
    raw = os.environ.get("PROXIES", "").strip()
    return [p.strip() for p in raw.split(",") if p.strip()] or None


def _read_csv(path, cols):
    if path.exists():
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        return df.reindex(columns=list(dict.fromkeys(list(df.columns) + cols)), fill_value="")
    return pd.DataFrame(columns=cols)


# ================================================================== search plan
def search_plan():
    """Every JobSpy search: (site, query, kwargs, market, sponsorship_query)."""
    s = CONFIG["sites"]
    plan = []
    for country, m in CONFIG["markets"].items():
        li = m.get("linkedin")
        if li:
            for loc in li["locations"]:
                for q in s["linkedin"]["query_sets"][li.get("queries", "full")]:
                    plan.append(("linkedin", q, dict(location=loc, results_wanted=li.get("results", s["linkedin"]["results_per_query"])),
                                 country, "sponsor" in li.get("queries", "")))
        ind = m.get("indeed")
        if ind:
            for q in s["indeed"]["query_sets"][ind.get("queries", "full")]:
                plan.append(("indeed", q, dict(location=ind.get("where") or None, country_indeed=ind["country"],
                                               results_wanted=ind.get("results", s["indeed"]["results_per_query"])),
                             country, "sponsor" in ind.get("queries", "")))
    b = s.get("bayt") or {}
    for loc in b.get("locations") or []:
        for q in b.get("queries") or []:
            plan.append(("bayt", q, dict(location=loc, results_wanted=b.get("results_per_query", 50)), None, False))
    return plan


def my_share(items, shard, shards, offset=0):
    return [x for i, x in enumerate(items) if (i + offset) % shards == shard]


class _BlockWatch(logging.Handler):
    def __init__(self):
        super().__init__()
        self.hits = 0

    def emit(self, record):
        m = record.getMessage().lower()
        if "429" in m or "999" in m or "blocked" in m or "403" in m:
            self.hits += 1


def jobspy_searches(plan, hours, deadline):
    from jobspy import scrape_jobs
    watch = _BlockWatch()
    for name in ("JobSpy:LinkedIn", "JobSpy:Indeed"):
        logging.getLogger(name).addHandler(watch)
    rows, log, stats = [], [], Counter()
    fails, blocked_streak, paused, skip, retry = Counter(), 0, False, set(), []

    def one(item, attempt):
        nonlocal blocked_streak
        site, q, kw, market, sponsor_q = item
        where = kw.get("location") or kw.get("country_indeed")
        watch.hits = 0
        try:
            df = scrape_jobs(site_name=[site], search_term=q, hours_old=hours, proxies=_proxies(),
                             fetch_description=CONFIG["fetch_description"], verbose=0, **kw)
            for r in df.to_dict("records"):
                desc = r.get("description")
                rows.append(dict(source=site, title=r.get("title"), company=r.get("company"),
                                 location=r.get("location"), job_url=fix_url(r.get("job_url")),
                                 date_posted=r.get("date_posted"), is_remote=bool(r.get("is_remote") is True),
                                 description=str(desc)[:8000] if isinstance(desc, str) else "",
                                 market=market, sponsor_query=sponsor_q))
            if site == "linkedin" and watch.hits and len(df) == 0:
                blocked_streak += 1
                if attempt == 0:
                    retry.append(item)
                    log.append(f"{site} | {where} | {q[:50]} | BLOCKED (will retry)")
                else:
                    stats["blocked"] += 1
                    log.append(f"{site} | {where} | {q[:50]} | BLOCKED again")
            else:
                stats["ok"] += 1
                blocked_streak = 0 if site == "linkedin" else blocked_streak
                log.append(f"{site} | {where} | {q[:50]} | {len(df)}" + (" (retry)" if attempt else ""))
        except Exception as e:
            fails[site] += 1
            stats["failed"] += 1
            log.append(f"{site} | {where} | {q[:50]} | FAILED: {str(e)[:100]}")
            if fails[site] >= 4:
                skip.add(site)
                log.append(f"{site}: skipped the rest on this runner after 4 errors")
        print(log[-1], flush=True)

    for item in plan:
        if time.time() > deadline:
            stats["skipped_time"] += 1
            continue
        if item[0] in skip:
            stats["skipped_blocked"] += 1
            continue
        one(item, 0)
        if blocked_streak >= 3:
            if not paused:
                log.append("linkedin: blocked 3 times in a row, waiting 2 minutes")
                time.sleep(120)
                paused, blocked_streak = True, 0
            else:
                skip.add("linkedin")
                log.append("linkedin: still blocked, skipped the rest on this runner")
        time.sleep(CONFIG["pause_seconds"])

    # blocked searches get one more try at the end, slower
    if retry and time.time() < deadline:
        log.append(f"linkedin: retrying {len(retry)} blocked searches after a 90 second wait")
        time.sleep(90)
        blocked_streak = 0
        for item in retry:
            if time.time() > deadline or blocked_streak >= 3:
                stats["blocked"] += 1
                continue
            one(item, 1)
            time.sleep(CONFIG["pause_seconds"] * 2)
    elif retry:
        stats["blocked"] += len(retry)
    return rows, log, stats


# ================================================================== collect (one runner)
def _sites_for_today(today, shards, shard):
    if not SITES.exists():
        return [], {}
    sites = pd.read_csv(SITES, dtype=str, keep_default_na=False)
    if "status" not in sites:
        return [], {}
    sites = sites[sites["status"] == "active"]
    state = {r["host"]: r for r in _read_csv(SITE_STATE, ["host"]).to_dict("records")}
    sites = sites.assign(_last=[state.get(h, {}).get("last_checked", "") for h in sites["host"]])
    sites = sites.sort_values("_last")
    per_day = int((CONFIG.get("site_reader") or {}).get("sites_per_day", 900))
    todays = sites.head(per_day).to_dict("records")
    return my_share(todays, shard, shards), state


def collect(shard=0, shards=1, role="all"):
    """role: 'search' (job boards, feeds), 'sites' (company websites from discovery) or 'all' (both)."""
    t0 = time.time()
    dcfg = CONFIG.get("daily") or {}
    private = os.environ.get("PRIVATE_REPO", "").lower() == "true"
    key = ("private_" if private else "") + ("site_minutes_per_shard" if role == "sites" else "minutes_per_shard")
    shard_minutes = float(dcfg.get(key, dcfg.get("minutes_per_shard", 70)))
    deadline = t0 + shard_minutes * 60
    today = today_pkt()
    backfill = lookback_days()
    hours = (backfill or 0) * 24 or CONFIG["hours_old"]
    window_lo = (today - timedelta(days=backfill or CONFIG["feed_window_days"])).isoformat()
    sm_days = max(backfill or 0, int((CONFIG.get("sitemaps") or {}).get("window_days", 30)))
    long_lo = (today - timedelta(days=sm_days)).isoformat()
    seen_links = set(_read_csv(SEEN_LINKS, ["key"])["key"])
    print(f"Runner {role} {shard + 1}/{shards} | {'BACKFILL ' + str(backfill) + ' days' if backfill else 'daily'}", flush=True)
    do_search, do_sites = role in ("all", "search"), role in ("all", "sites")

    # ---- JobSpy searches: each site's searches are dealt round-robin across runners
    plan = search_plan() if do_search else []
    mine = []
    for k, site in enumerate(("linkedin", "indeed")):
        mine += my_share([p for p in plan if p[0] == site], shard, shards, offset=k)
    if mine:   # start at a different search each day, so any searches cut by the time limit rotate
        k = today.toordinal() % len(mine)
        mine = mine[k:] + mine[:k]
    js_rows, log, js_stats = jobspy_searches(mine, hours, deadline - (6 if private else 15) * 60) if mine else ([], [], Counter())

    # ---- feeds and sites
    tasks_ok, tasks_failed = Counter(), Counter()
    feed_rows, opened, state_updates = [], [], {}

    def run_task(name, fn):
        nonlocal feed_rows, opened
        try:
            res = fn()
            got, got_log = res[0], res[1]
            if len(res) > 2 and res[2]:
                opened += res[2]
            feed_rows += got
            log.extend(got_log)
            bad = sum("FAILED" in l for l in got_log)
            tasks_ok[name] += 1 if bad == 0 or got else 0
            tasks_failed[name] += 1 if bad and not got else 0
        except Exception as e:
            tasks_failed[name] += 1
            log.append(f"{name} | FAILED: {str(e)[:120]}")
        print(f"{name} done ({time.time() - t0:.0f}s)", flush=True)

    if do_search:
        run_task("hiring systems", lambda: extra.hiring_systems(CONFIG, shard, shards))
    small = [
        ("remote boards", lambda: legacy_remote_boards()),
        ("remote job APIs", lambda: more.remote_apis(CONFIG, more_remote_ok())),
        ("job-board sitemaps", lambda: extra.sitemaps(CONFIG, role_group, long_lo)),
        ("aggregators", lambda: extra.aggregators(CONFIG)),
        ("telegram", lambda: more.telegram(CONFIG, window_lo)),
        ("alerts + web search", lambda: signals(today, window_lo, seen_links)),
        ("universities", lambda: more.universities(CONFIG, seen_links)),
    ]
    for name, fn in (my_share(small, shard, shards) if do_search else []):
        run_task(name, fn)

    sites, state = _sites_for_today(today, shards, shard) if do_sites else ([], {})
    if sites:
        sr = CONFIG.get("site_reader") or {}
        site_lo = (today - timedelta(days=max(backfill or 0, int(sr.get("window_days", 7))))).isoformat()
        rows, slog, op, upd = more.site_reader(sites, state, seen_links, site_lo, int(sr.get("pages_per_site", 6)),
                                               deadline - 5 * 60)
        feed_rows += rows
        log += slog
        opened += op
        state_updates.update(upd)
        tasks_ok["company sites"] += 1
    elif do_sites:
        log.append("company sites | none active yet (run the weekly discovery workflow)")

    # ---- dates: feeds count only recent jobs
    def recent(r):
        d = str(r.get("date_posted") or "")[:10]
        return not d or d >= (long_lo if r["source"] in LONG_WINDOW else window_lo)
    feed_rows = [r for r in feed_rows if recent(r)]

    # ---- match, then check the new ones
    matched, raw_n, dropped_loc, blank = match_rows(js_rows + feed_rows)
    if blank:   # matching titles with no usable location: read it from the job page
        filled, fill_log = fill_locations(blank, int(dcfg.get("location_lookups_per_shard", 40)), deadline)
        log += fill_log
        more_matched, _, more_dropped, _ = match_rows(filled, count_blank=False)
        matched += more_matched
        dropped_loc.update(more_dropped)
    screened, checks_log = screen(matched, today, deadline)
    log += checks_log

    OUT.mkdir(exist_ok=True)
    out = dict(shard=shard, shards=shards, role=role, raw=raw_n, rows=[r for r in matched if r.get("keep")],
               known=[r["job_id"] for r in matched if r.get("known")],
               screened=screened, opened=sorted(set(opened)), site_state=state_updates, log=log,
               dropped_location=dict(dropped_loc.most_common(40)),
               stats=dict(searches_planned=len(mine), searches_ok=js_stats["ok"], searches_failed=js_stats["failed"],
                          searches_blocked=js_stats["blocked"], searches_skipped=js_stats["skipped_time"] + js_stats["skipped_blocked"],
                          sources_ok=sum(tasks_ok.values()), sources_failed=sum(tasks_failed.values()),
                          minutes=round((time.time() - t0) / 60, 1)))
    (OUT / f"shard_{role}_{shard}.json").write_text(json.dumps(out, ensure_ascii=False, default=str))
    print(f"Runner {shard + 1}: raw={raw_n} matched={len(matched)} kept={len(out['rows'])} in {out['stats']['minutes']} min")
    return out


def more_remote_ok():
    from .match import REMOTE_OK
    return REMOTE_OK


def legacy_remote_boards():
    """RemoteOK + We Work Remotely."""
    import xml.etree.ElementTree as ET
    from .match import REMOTE_OK
    rows, log = [], []
    wanted = CONFIG.get("remote_boards") or []
    ua = {"User-Agent": extra.UA}
    if "remoteok" in wanted:
        try:
            r = requests.get("https://remoteok.com/api", headers=ua, timeout=45)
            r.raise_for_status()
            r.encoding = "utf-8"
            got = [dict(source="remoteok", title=j.get("position"), company=j.get("company"),
                        location=(j.get("location") or "Remote") + " | Remote", job_url=j.get("url"),
                        date_posted=extra.to_date(j.get("date") or j.get("epoch")), is_remote=True)
                   for j in r.json() if isinstance(j, dict) and "position" in j
                   and REMOTE_OK.search(j.get("location") or "")]
            rows += got
            log.append(f"remoteok | {len(got)}")
        except Exception as e:
            log.append(f"remoteok | FAILED: {str(e)[:100]}")
    if "weworkremotely" in wanted:
        try:
            r = requests.get("https://weworkremotely.com/remote-jobs.rss", headers=ua, timeout=45)
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
                got.append(dict(source="weworkremotely", title=html.unescape(title), company=html.unescape(company),
                                location=f"{region} | Remote", job_url=g("link"),
                                date_posted=extra.to_date(g("pubDate")), is_remote=True))
            rows += got
            log.append(f"weworkremotely | {len(got)}")
        except Exception as e:
            log.append(f"weworkremotely | FAILED: {str(e)[:100]}")
    return rows, log


def signals(today, window_lo, seen_links):
    cands, log = more.candidates_alerts(CONFIG, window_lo)
    c2, l2 = more.candidates_brave(CONFIG, today)
    cands += c2
    log += l2
    budget = int((CONFIG.get("signals") or {}).get("max_pages_opened", 60))
    rows, l3, opened = more.open_candidates(cands, seen_links, budget)
    return rows, log + l3, opened


BLANK_LOC = re.compile(r"^\s*(\d+\s+locations?|multiple locations|various locations|n/?a|none|nan)?\s*$", re.I)


def fill_locations(rows, budget, deadline):
    """Open the job page of matching titles that came without a location and read it there."""
    from concurrent.futures import ThreadPoolExecutor
    todo = [r for r in rows if str(r.get("job_url") or "").startswith("http")][:budget]

    def go(r):
        if time.time() > deadline:
            return r, ""
        try:
            got = more.open_job_page(r["job_url"], r["source"])
            loc = next((g["location"] for g in got if g.get("location")), "")
            return r, loc
        except Exception:
            return r, ""

    found = 0
    out = []
    with ThreadPoolExecutor(8) as pool:
        for r, loc in pool.map(go, todo):
            if loc:
                found += 1
                out.append({**r, "location": loc})
    return out, [f"locations | missing {len(rows)} | looked up {len(todo)} | found {found}"]


def match_rows(raw_rows, count_blank=True):
    """Role + place matching. Returns (matched rows, raw count, Counter of locations dropped,
    rows with a matching title but no usable location)."""
    hist = _read_csv(JOBS, ["job_id", "job_url", "last_seen"])
    win = (today_pkt() - timedelta(days=CONFIG["repost_window_days"])).isoformat()
    recent = hist[hist["last_seen"] >= win]
    known_ids, known_urls = set(recent["job_id"]), set(recent["job_url"].map(fix_url))
    out, dropped, blank = [], Counter(), []
    seen_urls = set()
    for r in raw_rows:
        title = html.unescape(str(r.get("title") or "")).strip()
        company = str(r.get("company") or "").strip()
        url = fix_url(r.get("job_url"))
        if not title or not url or company in ("", "nan", "None"):
            continue
        group = role_group(title)
        if not group:
            continue
        city, country = GEO.place(r.get("location"), r.get("is_remote") is True)
        if not city:
            if count_blank and BLANK_LOC.match(str(r.get("location") or "")) and r.get("source") not in JOBSPY_SITES:
                blank.append(r)
            else:
                dropped[str(r.get("location") or "")[:60]] += 1
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        d = pd.to_datetime(r.get("date_posted"), errors="coerce")
        jid = job_id(company, title, city)
        out.append(dict(job_id=jid, title=title, company=company, city=city, country=country or "",
                        region=GEO.region(city, country), role_group=group, level=level(title),
                        source=r["source"], job_url=url, location_raw=str(r.get("location") or ""),
                        date_posted="" if pd.isna(d) else d.date().isoformat(),
                        description=r.get("description") or "", sponsor_query=bool(r.get("sponsor_query")),
                        known=jid in known_ids or url in known_urls, note=""))
    return out, len(raw_rows), dropped, blank


def screen(rows, today, deadline):
    """Visa / nationals rules for jobs not seen before. Sets keep + note on each row.
    Returns (screened records for the cache, log lines)."""
    cache = _read_csv(SCREENED, ["job_id", "date", "reason"])
    old = set(cache.loc[cache["date"] >= (today - timedelta(days=45)).isoformat(), "job_id"])
    budget = int((CONFIG.get("daily") or {}).get("checks_per_shard", 90))
    order = {"required": 0, "open": 1, "gulf": 2}
    todo = []
    stats, screened = Counter(), []
    for r in rows:
        if r["known"]:
            r["keep"] = False
            continue
        if r["job_id"] in old:
            r["keep"] = False
            stats["dropped earlier"] += 1
            continue
        if nationals_only(r["title"]):
            r["keep"] = False
            screened.append(dict(job_id=r["job_id"], date=today.isoformat(), reason="nationals only (title)"))
            stats["nationals only"] += 1
            continue
        r["keep"] = True
        rule = GEO.visa_rule(r["country"]) if r["city"] != "Remote" else "none"
        if rule in order and r["region"] != "Pakistan":
            todo.append((order[rule], r))
    todo.sort(key=lambda x: x[0])
    fetched = 0
    for _, r in todo:
        rule = GEO.visa_rule(r["country"])
        desc = r.get("description") or ""
        if not desc and fetched < budget and time.time() < deadline:
            desc = more.describe(r["job_url"])
            fetched += 1
            if "linkedin.com" in r["job_url"]:
                time.sleep(1.5)
        if desc and nationals_only("", desc):
            r["keep"] = False
            screened.append(dict(job_id=r["job_id"], date=today.isoformat(), reason="nationals only (ad)"))
            stats["nationals only"] += 1
            continue
        says = visa_says(desc)
        if rule == "required":
            on_list = SPONSORS.has(GEO.sponsor_list(r["country"]), r["company"])
            if says == "no":
                r["keep"] = False
                screened.append(dict(job_id=r["job_id"], date=today.isoformat(), reason="ad rules out visa"))
                stats["visa ruled out"] += 1
            elif says == "yes":
                r["note"] = "ad mentions visa / relocation"
            elif on_list:
                r["note"] = f"licensed visa sponsor ({GEO.sponsor_list(r['country']).upper()} list)"
            elif desc:
                r["keep"] = False
                screened.append(dict(job_id=r["job_id"], date=today.isoformat(), reason="no visa mention"))
                stats["no visa mention"] += 1
            elif r["sponsor_query"]:
                r["note"] = "found by a visa-sponsorship search; check the ad"
            else:
                r["keep"] = False
                stats["visa not checked (ad unreadable)"] += 1
        elif rule == "open":
            if says == "no":
                r["keep"] = False
                screened.append(dict(job_id=r["job_id"], date=today.isoformat(), reason="ad rules out visa"))
                stats["visa ruled out"] += 1
            elif says == "yes":
                r["note"] = "ad mentions visa / relocation"
    for r in rows:
        r.pop("description", None)
    return screened, [f"checks | descriptions opened {fetched} | {json.dumps(dict(stats))}"]


# ================================================================== merge (after all runners)
def fuzzy_duplicates(fresh, history, already, within=False):
    """Same city + company names >= 90% similar + titles >= 92% similar = duplicate
    (e.g. "Sr. Data Analyst" at "Systems Ltd" vs "Senior Data Analyst" at "Systems Limited")."""
    from rapidfuzz import fuzz

    def t_norm(x):
        return norm(re.sub(r"\bsr\.?\s", "senior ", str(x or ""), flags=re.I))

    out = pd.Series(False, index=fresh.index)
    pool = {}
    for _, h in history.iterrows():
        pool.setdefault(h.get("city", ""), []).append((norm(h.get("company")), t_norm(h.get("title"))))
    for i, r in fresh.iterrows():
        if already.get(i, False):
            continue
        c, t = norm(r["company"]), t_norm(r["title"])
        for hc, ht in pool.get(r["city"], []):
            if fuzz.token_set_ratio(c, hc) >= 90 and fuzz.token_set_ratio(t, ht) >= 92:
                out[i] = True
                break
        if within and not out[i]:
            pool.setdefault(r["city"], []).append((c, t))
    return out


def merge(shard_dir=OUT):
    today = today_pkt()
    t = today.isoformat()
    backfill = lookback_days()
    parts = [json.loads(p.read_text()) for p in sorted(Path(shard_dir).rglob("shard_*.json"))]
    if not parts:
        raise SystemExit("No runner results found.")
    log, stats, dropped = [], Counter(), Counter()
    rows, known, screened, opened, site_state = [], set(), [], [], {}
    for p in parts:
        rows += p["rows"]
        known |= set(p["known"])
        screened += p["screened"]
        opened += p["opened"]
        site_state.update(p["site_state"])
        log += [f"--- {p.get('role', 'runner')} runner {p['shard'] + 1}/{p['shards']} ({p['stats']['minutes']} min)"] + p["log"]
        stats.update({k: v for k, v in p["stats"].items() if k != "minutes"})
        stats["raw"] += p["raw"]
        dropped.update(p["dropped_location"])

    fresh = pd.DataFrame(rows).reindex(columns=COLUMNS + ["country"]).fillna("")
    if len(fresh):
        fresh = fresh.drop_duplicates("job_url").drop_duplicates("job_id")
        fresh = fresh[~fuzzy_duplicates(fresh, fresh.iloc[0:0], pd.Series(False, index=fresh.index), within=True)]

    hist = _read_csv(JOBS, COLUMNS)
    if len(hist):
        hist["job_url"] = hist["job_url"].map(fix_url)
    win = (today - timedelta(days=CONFIG["repost_window_days"])).isoformat()
    recent = hist[hist["last_seen"] >= win]
    seen = fresh["job_id"].isin(set(recent["job_id"])) | fresh["job_url"].isin(set(recent["job_url"]))
    seen = seen | fuzzy_duplicates(fresh, recent, seen)
    new = fresh[~seen].copy()
    window_lo = (today - timedelta(days=backfill or CONFIG["feed_window_days"])).isoformat()
    new["first_seen"], new["last_seen"] = t, t
    if backfill:   # looked further back: date these by when they were posted
        new["first_seen"] = [p if p and window_lo <= p <= t else t for p in new["date_posted"]]
    hist.loc[hist["job_id"].isin(known | set(fresh.loc[seen, "job_id"])), "last_seen"] = t
    companies_before = set(hist["company"].map(norm))
    hist = pd.concat([hist, new.reindex(columns=COLUMNS)], ignore_index=True).fillna("")
    DATA.mkdir(exist_ok=True)
    hist.reindex(columns=COLUMNS).to_csv(JOBS, index=False)

    # small state files
    sc = _read_csv(SCREENED, ["job_id", "date", "reason"])
    sc = pd.concat([sc, pd.DataFrame(screened, columns=["job_id", "date", "reason"])], ignore_index=True)
    sc = sc[sc["date"] >= (today - timedelta(days=45)).isoformat()].drop_duplicates("job_id", keep="last")
    sc.to_csv(SCREENED, index=False)
    sl = _read_csv(SEEN_LINKS, ["key", "date"])
    sl = pd.concat([sl, pd.DataFrame({"key": opened, "date": t})], ignore_index=True)
    sl = sl[sl["date"] >= (today - timedelta(days=120)).isoformat()].drop_duplicates("key")
    sl.to_csv(SEEN_LINKS, index=False)
    if site_state:
        ss = {r["host"]: r for r in _read_csv(SITE_STATE, ["host"]).to_dict("records")}
        for h, st in site_state.items():
            ss[h] = {**ss.get(h, {}), **st, "host": h}
        cols = ["host", "etag", "last_modified", "hash", "last_checked", "last_found", "fails"]
        pd.DataFrame(list(ss.values())).reindex(columns=cols).fillna("").to_csv(SITE_STATE, index=False)

    # summary + log
    planned = stats["searches_planned"] + stats["sources_ok"] + stats["sources_failed"]
    done = stats["searches_ok"] + stats["sources_ok"]
    coverage = round(100 * done / planned) if planned else 0
    cities_hit = sorted(set(fresh["city"])) if len(fresh) else []
    summary = (f"raw={stats['raw']}  matching={len(fresh)}  new={len(new)}  total={len(hist)}  coverage={coverage}%\n"
               f"Searches: {stats['searches_ok']}/{stats['searches_planned']} done, {stats['searches_failed']} failed, "
               f"{stats['searches_blocked']} blocked, {stats['searches_skipped']} skipped\n"
               f"New by city: {new['city'].value_counts().to_dict() if len(new) else {}}\n"
               f"New by role group: {new['role_group'].value_counts().to_dict() if len(new) else {}}\n"
               f"Cities with matching jobs today: {', '.join(cities_hit)}")
    print("\n" + summary)
    LAST_RUN.write_text(f"Run at {datetime.now(PKT):%Y-%m-%d %H:%M} PKT "
                        f"({'look back ' + str(backfill) + ' days' if backfill else 'daily'}, {len(parts)} runners)\n"
                        + summary + "\n\nMatching titles dropped for location (top 25): "
                        + json.dumps(dict(dropped.most_common(25)), ensure_ascii=False) + "\n\n" + "\n".join(log) + "\n")

    # ---- Sheet: append only. Existing rows (and your notes in them) are never touched.
    def why(r):
        return f"{r.role_group} · {r.note}" if r.note else r.role_group

    if len(new):
        new = new.sort_values(["first_seen", "date_posted"], ascending=False)
        sheets.write("master", TAB, SHEET_HEADERS,
                     [[r.first_seen, r.date_posted, r.title, r.company, r.city, why(r), r.level,
                       SOURCE_NAMES.get(r.source, r.source), r.job_url, "", r.region] for r in new.itertuples()],
                     format=False)
        nc = new[~new["company"].map(norm).isin(companies_before)]
        if len(nc):
            g = nc.groupby(nc["company"].map(norm))
            sheets.write("master", "Companies newly hiring",
                         ["Date", "Company", "Roles found", "Cities", "Region", "Example role", "Link"],
                         [[t, x["company"].iloc[0], len(x), ", ".join(sorted(set(x["city"]))),
                           ", ".join(sorted(set(x["region"]))), x["title"].iloc[0], x["job_url"].iloc[0]]
                          for _, x in g], format=False)
    sheets.write("master", "Run health",
                 ["Run at (PKT)", "Mode", "Jobs scanned", "Matching", "New", "Searches done", "Searches planned",
                  "Failed or blocked", "Sources OK", "Sources failed", "Coverage %", "Cities with matches",
                  "Dropped by visa / nationals checks", "Runners"],
                 [[f"{datetime.now(PKT):%Y-%m-%d %H:%M}", f"look back {backfill}d" if backfill else "daily",
                   stats["raw"], len(fresh), len(new), stats["searches_ok"], stats["searches_planned"],
                   stats["searches_failed"] + stats["searches_blocked"], stats["sources_ok"], stats["sources_failed"],
                   coverage, len(cities_hit), len(screened), len(parts)]], format=False)
    if stats["raw"] == 0:
        raise SystemExit("No jobs found at all: every source failed. Check data/last_run.txt.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("step", nargs="?", default="all", choices=["all", "collect", "merge"])
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--from", dest="src", default=str(OUT))
    ap.add_argument("--role", default="all", choices=["all", "search", "sites"])
    a = ap.parse_args()
    if a.step in ("all", "collect"):
        collect(a.shard, a.shards, a.role)
    if a.step in ("all", "merge"):
        merge(Path(a.src))


if __name__ == "__main__":
    main()
