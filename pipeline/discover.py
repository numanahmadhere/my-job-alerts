"""Discovery: find employers and company websites that publish jobs, so the daily run can read them.

  python -m pipeline.discover weekly    visa-sponsor lists, certificate logs (new careers.* sites),
                                         check new websites, check employers' job boards
  python -m pipeline.discover monthly   Common Crawl sweep (hiring-system boards + careers pages in
                                         your countries' web addresses), then the weekly checks
  python -m pipeline.discover wdc       one-time import of sites that use job markup (Web Data Commons)

Writes only: data/sites.csv, data/employers.csv, data/sponsors/*.txt, data/discovery_state.json,
data/discovery_log.txt (the daily run writes different files, so the two never clash).
"""
import csv
import gzip
import io
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import requests

from . import ats2, extra  # noqa: F401  (ats2 registers the new readers)
from .match import CONFIG, GEO, ROOT, description_from_html, sponsor_norm
from .more import BROWSER_UA, _text, jsonld_jobs

DATA = ROOT / "data"
SITES = DATA / "sites.csv"
EMPLOYERS = DATA / "employers.csv"
STATE = DATA / "discovery_state.json"
LOG = DATA / "discovery_log.txt"
SITE_COLS = ["host", "careers_url", "kind", "sitemap_url", "rss_url", "job_url_pattern", "name",
             "default_location", "country", "status", "priority", "source", "added", "probed", "ats"]
EMP_COLS = ["platform", "slug", "name", "status", "target_jobs", "source", "added", "last_checked"]
DISC = CONFIG.get("discovery") or {}
import os
SCALE = float(DISC.get("private_scale", 0.2)) if os.environ.get("PRIVATE_REPO", "").lower() == "true" else 1.0
TODAY = date.today().isoformat()
TLD_COUNTRY = {"pk": "Pakistan", "ae": "United Arab Emirates", "sa": "Saudi Arabia", "qa": "Qatar", "bh": "Bahrain",
               "om": "Oman", "kw": "Kuwait", "jo": "Jordan", "eg": "Egypt", "my": "Malaysia", "sg": "Singapore",
               "jp": "Japan", "uk": "United Kingdom", "ie": "Ireland", "de": "Germany", "nl": "Netherlands",
               "pl": "Poland", "hu": "Hungary", "cz": "Czechia", "pt": "Portugal", "tr": "Turkey", "ca": "Canada",
               "au": "Australia", "nz": "New Zealand"}
GULF_FIRST = ["pk", "ae", "sa", "qa", "bh", "om", "kw", "jo", "eg"]
_log = []


def log(msg):
    print(msg, flush=True)
    _log.append(msg)


# ---------------------------------------------------------------- csv helpers
def read_rows(path, cols):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return [{c: r.get(c, "") or "" for c in cols} for r in csv.DictReader(f)]


def write_rows(path, cols, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})


def tld_of(host):
    return host.rsplit(".", 1)[-1].lower() if "." in host else ""


def add_sites(new_sites, source):
    sites = read_rows(SITES, SITE_COLS)
    have = {s["host"] for s in sites}
    added = 0
    for s in new_sites:
        h = s["host"].lower().strip(".")
        if not h or h in have:
            continue
        tld = tld_of(h)
        sites.append(dict(host=h, careers_url=s.get("careers_url") or f"https://{h}/", status="new",
                          priority=s.get("priority") or ("1" if tld in GULF_FIRST else "2" if tld in TLD_COUNTRY else "3"),
                          country=TLD_COUNTRY.get(tld, ""), default_location=TLD_COUNTRY.get(tld, ""),
                          source=source, added=TODAY, name=s.get("name", "")))
        have.add(h)
        added += 1
    write_rows(SITES, SITE_COLS, sites)
    log(f"{source} | new websites added {added} (total {len(sites)})")


def add_employers(found, source):
    emps = read_rows(EMPLOYERS, EMP_COLS)
    have = {(e["platform"], e["slug"].lower()) for e in emps}
    added = 0
    for platform, slug, name in found:
        key = (platform, slug.lower())
        if not slug or key in have:
            continue
        emps.append(dict(platform=platform, slug=slug, name=name or slug, status="new", source=source, added=TODAY))
        have.add(key)
        added += 1
    write_rows(EMPLOYERS, EMP_COLS, emps)
    log(f"{source} | new employers added {added} (total {len(emps)})")


# ---------------------------------------------------------------- hiring-system links on a page
SKIP_SLUGS = {"www", "api", "app", "help", "support", "cdn", "embed", "assets", "js", "static", "jobs", "careers",
              "apply", "blog", "status", "marketplace", "partners", "go", "success", "resources", "j", "v1", "login",
              "wday", "en-us", "job", "career", "sr-jobs", "boards", "job-boards", "developers", "docs", "info"}
ATS_PATTERNS = [
    ("greenhouse", r"(?:boards|job-boards)\.greenhouse\.io/(?:embed/job_board(?:/js)?\?for=)?([A-Za-z0-9_-]+)"),
    ("greenhouse", r"greenhouse\.io/embed/job_board(?:/js)?\?for=([A-Za-z0-9_-]+)"),
    ("lever", r"jobs\.lever\.co/([A-Za-z0-9_.-]+)"),
    ("workable", r"apply\.workable\.com/([A-Za-z0-9_-]+)"),
    ("ashby", r"jobs\.ashbyhq\.com/([A-Za-z0-9_.%-]+)"),
    ("smartrecruiters", r"(?:careers|jobs)\.smartrecruiters\.com/([A-Za-z0-9_-]+)"),
    ("recruitee", r"([a-z0-9-]+)\.recruitee\.com"),
    ("bamboohr", r"([a-z0-9-]+)\.bamboohr\.com"),
    ("personio", r"([a-z0-9-]+)\.jobs\.personio\.(?:de|com)"),
    ("breezy", r"([a-z0-9-]+)\.breezy\.hr"),
    ("teamtailor", r"([a-z0-9-]+)\.teamtailor\.com"),
    ("pinpoint", r"([a-z0-9-]+)\.pinpointhq\.com"),
]
WORKDAY_RX = re.compile(r"([a-z0-9-]+)\.(wd\d+)\.myworkdayjobs\.com/(?:[a-z]{2}-[A-Z]{2}/)?([A-Za-z0-9_-]+)")


NEW_ATS = [
    ("oracle", re.compile(r"([a-z0-9-]+\.fa\.[a-z0-9-]+\.oraclecloud\.com)/hcmUI/CandidateExperience/[a-z]{2}(?:-[A-Z]{2})?/sites/([A-Za-z0-9_]+)")),
    ("successfactors", re.compile(r"(career\d*\.successfactors\.(?:com|eu))/career\?company=([A-Za-z0-9_-]+)")),
    ("taleo", re.compile(r"([a-z0-9-]+)\.taleo\.net/careersection/([A-Za-z0-9_.-]+)/")),
    ("icims", re.compile(r"((?:careers-|jobs-)?[a-z0-9-]+\.icims\.com)")),
]


def find_ats(text, host=None):
    out = set()
    for platform, rx in NEW_ATS:
        for m in rx.finditer(text):
            if platform == "icims":
                h = m.group(1).lower()
                if h.split(".")[0] not in SKIP_SLUGS and h != "www.icims.com":
                    out.add(("icims", h, h.split(".")[0].replace("careers-", "")))
            elif platform == "taleo":
                if m.group(2).lower() not in ("rest", "application", "theme"):
                    out.add(("taleo", f"{m.group(1)}|{m.group(2)}", m.group(1)))
            else:
                out.add((platform, f"{m.group(1)}|{m.group(2)}",
                         m.group(2) if platform == "successfactors" else m.group(1).split(".")[0]))
    if host and re.search(r"rmkcdn|jobs2web|successfactors\.com/career|sfcareer", text):
        out.add(("successfactors", host, host))   # SAP Career Site Builder on the company's own domain
    for platform, rx in ATS_PATTERNS:
        for m in re.finditer(rx, text):
            slug = unquote(m.group(1)).strip("/")
            if slug.lower() not in SKIP_SLUGS and len(slug) > 1:
                out.add((platform, slug, slug))
    for m in WORKDAY_RX.finditer(text):
        if m.group(3).lower() not in SKIP_SLUGS:
            out.add(("workday", f"{m.group(1)}/{m.group(2)}/{m.group(3)}", m.group(1)))
    return out


# ---------------------------------------------------------------- website check (probe)
JOBISH = re.compile(r"(?i)/(jobs?|careers?|vacanc\w*|positions?|openings?|stellen\w*|vacatures?|job-?details?|requisitions?)/")
CAREER_LINK = re.compile(r"(?i)(career|jobs|vacanc|join[- ]?us|work[- ]with[- ]us|karriere|stellen|vacature|werken|kariera|kariyer|recruit|وظائف)")


def _get(url, timeout=25):
    from curl_cffi import requests as cr
    r = cr.get(url, impersonate="chrome", timeout=timeout, allow_redirects=True)
    if r.status_code >= 400:
        return None, str(r.url)
    c = r.content
    if c[:2] == b"\x1f\x8b":
        c = gzip.decompress(c)
    return c, str(r.url)


def _relevant(jobs):
    return any(GEO.place(j.get("location"), j.get("remote"))[0] for j in jobs)


def probe(site):
    """Decide how (and whether) the daily run can read this website."""
    host = site["host"]
    res = dict(site)
    res.update(probed=TODAY, status="none", kind="", ats="")
    target_tld = tld_of(host) in TLD_COUNTRY or site.get("source") == "target"
    try:
        page, final = _get(site.get("careers_url") or f"https://{host}/")
        if page is None:
            page, final = _get(f"https://{host}/")
        if page is None:
            res["status"] = "unreachable"
            return res, set()
        text = _text(page)
        # homepage? follow its careers link once
        if not jsonld_jobs(page) and not JOBISH.search(final or ""):
            for href, label in re.findall(r'<a[^>]+href=["\']([^"\'#]+)["\'][^>]*>(.*?)</a>', text, re.S | re.I)[:400]:
                if CAREER_LINK.search(href) or CAREER_LINK.search(description_from_html(label)):
                    full = urljoin(final, href)
                    if urlparse(full).netloc.endswith(host.split(".", 1)[-1]) or find_ats(full):
                        p2, f2 = _get(full)
                        if p2:
                            page, final, text = p2, f2, _text(p2)
                        break
        ats = find_ats(text, urlparse(final or "").netloc or host) | find_ats(final or "")
        if site.get("name"):   # target employers: keep their real name instead of the system's code
            ats = {(p, s, site["name"]) for p, s, _ in ats}
        res["careers_url"] = final or res.get("careers_url")
        jobs = jsonld_jobs(page, final)
        rss = re.search(r'<link[^>]+type=["\']application/(?:rss|atom)\+xml["\'][^>]*href=["\']([^"\']+)["\']', text, re.I)
        # sitemaps
        sm_url, sm_jobs = "", []
        try:
            robots, _ = _get(f"https://{host}/robots.txt", 15)
            sms = re.findall(r"(?im)^\s*sitemap:\s*(\S+)", _text(robots or b""))
        except Exception:
            sms = []
        sms = sms[:4] or [f"https://{host}/sitemap.xml"]
        for sm in sms:
            try:
                c, _ = _get(sm, 30)
                if not c:
                    continue
                entries, is_index = extra._sitemap_urls(c)
                if is_index:
                    kids = [l for l, _ in entries if l and re.search(r"(?i)job|career|vacanc|position|stellen|vacature", l)]
                    if not kids:
                        continue
                    c2, _ = _get(kids[0], 30)
                    entries = extra._sitemap_urls(c2)[0] if c2 else []
                    sm = kids[0]
                urls = [l for l, _ in entries if l and JOBISH.search(l)]
                if len(urls) >= 2:
                    sm_url, sm_jobs = sm, urls
                    break
            except Exception:
                continue
        if sm_url:
            # check a couple of job pages for job markup / location
            sample = []
            for u in sm_jobs[:2]:
                try:
                    p, _ = _get(u)
                    sample += jsonld_jobs(p or b"", u)
                except Exception:
                    pass
            if target_tld or (sample and _relevant(sample)):
                res.update(status="active", kind="sitemap", sitemap_url=sm_url)
            elif sample:
                res["status"] = "offtarget"
        if res["status"] != "active" and jobs:
            if target_tld or _relevant(jobs):
                res.update(status="active", kind="careers")
            else:
                res["status"] = "offtarget"
        if res["status"] not in ("active", "offtarget") and rss and CAREER_LINK.search(rss.group(1) + (final or "")):
            res.update(status="active", kind="rss", rss_url=urljoin(final, rss.group(1)))
        if res["status"] == "none" and target_tld and CAREER_LINK.search(final or ""):
            # a careers page in your countries without job markup: read its job links daily
            res.update(status="active", kind="careers")
        if ats:
            res["ats"] = ";".join(sorted(f"{p}:{s}" for p, s, _ in ats))[:500]
            if res["status"] in ("none", "offtarget"):
                res["status"] = "ats"
        t = re.search(r"<title[^>]*>(.*?)</title>", text, re.S | re.I)
        if t and not res.get("name"):
            res["name"] = re.split(r"\s[|\-–]\s", description_from_html(t.group(1)))[-1][:60]
        return res, ats
    except Exception as e:
        res["status"] = "error"
        return res, set()


def probe_new(minutes, limit):
    sites = read_rows(SITES, SITE_COLS)
    todo = [s for s in sites if s["status"] in ("", "new")]
    stale = (date.today() - timedelta(days=90)).isoformat()
    todo += [s for s in sites if s["status"] in ("offtarget", "none", "unreachable", "error") and s["probed"] < stale]
    todo.sort(key=lambda s: (s["priority"] or "3", s["added"]))
    todo = todo[:limit]
    deadline = time.time() + minutes * 60
    log(f"probe | websites to check {len(todo)}")
    results, all_ats = {}, set()

    def go(s):
        if time.time() > deadline:
            return None, set()
        return probe(s)

    with ThreadPoolExecutor(24) as pool:
        for res, ats in pool.map(go, todo):
            if res:
                results[res["host"]] = res
                all_ats |= ats
    sites = [results.get(s["host"], s) for s in sites]
    write_rows(SITES, SITE_COLS, sites)
    counts = {}
    for r in results.values():
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    log(f"probe | checked {len(results)} | {json.dumps(counts)}")
    if all_ats:
        add_employers(all_ats, "website")


# ---------------------------------------------------------------- employer check
def _workday_locations(slug):
    tenant, shard, site = slug.split("/")
    data = extra._post_json(f"https://{tenant}.{shard}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs",
                            {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""}) or {}
    names = [j.get("locationsText", "") for j in data.get("jobPostings") or []]

    def walk(x):
        if isinstance(x, dict):
            if "descriptor" in x:
                names.append(str(x["descriptor"]))
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(data.get("facets") or [])
    return names, data.get("total", 0)


def check_employer(e):
    out = dict(e, last_checked=TODAY)
    try:
        if e["platform"] == "workday":
            names, total = _workday_locations(e["slug"])
            hits = sum(1 for n in names if GEO.place(n)[0])
            out.update(target_jobs=str(hits), status="active" if hits else "inactive" if total else "empty")
        else:
            rows = extra.FETCHERS[e["platform"]](e["slug"], e["name"])
            hits = sum(1 for r in rows if GEO.place(r["location"], r["is_remote"])[0])
            out.update(target_jobs=str(hits), status="active" if hits else "inactive" if rows else "empty")
            if rows and rows[0].get("company") and out["name"] == out["slug"]:
                out["name"] = rows[0]["company"]
    except Exception as e2:
        out["status"] = "error"
    return out


def validate(minutes):
    emps = read_rows(EMPLOYERS, EMP_COLS)
    d14 = (date.today() - timedelta(days=14)).isoformat()
    d45 = (date.today() - timedelta(days=45)).isoformat()
    due = [i for i, e in enumerate(emps)
           if e["status"] in ("", "new") or (e["status"] == "active" and e["last_checked"] < d14)
           or (e["status"] in ("inactive", "empty", "error") and e["last_checked"] < d45)]
    due.sort(key=lambda i: (emps[i]["status"] not in ("", "new"), emps[i]["last_checked"]))
    deadline = time.time() + minutes * 60
    log(f"employers | to check {len(due)}")

    def go(i):
        return i, (check_employer(emps[i]) if time.time() < deadline else None)

    done = 0
    with ThreadPoolExecutor(16) as pool:
        for i, res in pool.map(go, due):
            if res:
                emps[i] = res
                done += 1
    write_rows(EMPLOYERS, EMP_COLS, emps)
    counts = {}
    for e in emps:
        counts[e["status"]] = counts.get(e["status"], 0) + 1
    log(f"employers | checked {done} | {json.dumps(counts)}")


# ---------------------------------------------------------------- visa sponsor lists
def sponsors():
    out = DATA / "sponsors"
    out.mkdir(parents=True, exist_ok=True)
    h = {"User-Agent": BROWSER_UA}
    # UK: Home Office register of licensed sponsors (workers), updated every working day
    try:
        meta = requests.get("https://www.gov.uk/api/content/government/publications/register-of-licensed-sponsors-workers",
                            headers=h, timeout=60).text
        url = re.search(r"https://assets\.publishing\.service\.gov\.uk/[^\"']+\.csv", meta).group(0)
        r = requests.get(url, headers=h, timeout=300)
        r.raise_for_status()
        rows = csv.DictReader(io.StringIO(r.content.decode("utf-8-sig", "ignore")))
        names = {sponsor_norm(x.get("Organisation Name")) for x in rows if "skilled" in str(x.get("Route", "")).lower()}
        names.discard("")
        (out / "uk.txt").write_text("\n".join(sorted(names)))
        log(f"sponsors | UK | {len(names)}")
    except Exception as e:
        log(f"sponsors | UK | FAILED: {str(e)[:120]}")
    # Netherlands: IND public register of recognised sponsors (highly skilled migrants)
    try:
        from bs4 import BeautifulSoup
        r = requests.get("https://ind.nl/en/public-register-recognised-sponsors/public-register-regular-labour-and-highly-skilled-migrants",
                         headers=h, timeout=120)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        names = {sponsor_norm(tr.find("td").get_text()) for tr in soup.select("table tr") if tr.find("td")}
        names.discard("")
        (out / "nl.txt").write_text("\n".join(sorted(names)))
        log(f"sponsors | NL | {len(names)}")
    except Exception as e:
        log(f"sponsors | NL | FAILED: {str(e)[:120]}")
    # Canada: employers with positive work-permit (LMIA) decisions, open.canada.ca
    try:
        pkg = requests.get("https://open.canada.ca/data/api/action/package_show?id=90fed587-1364-4f33-a9ee-208181dc0b97",
                           headers=h, timeout=60).json()["result"]["resources"]
        csvs = [x for x in pkg if str(x.get("format", "")).upper() == "CSV" and "en" in (x.get("language") or ["en"])]
        csvs.sort(key=lambda x: x.get("created") or "", reverse=True)
        names = set()
        for res in csvs[:4]:
            r = requests.get(res["url"], headers=h, timeout=180)
            text = r.content.decode("utf-8-sig", "ignore") if r.ok else ""
            lines = text.splitlines()
            start = next((i for i, l in enumerate(lines[:10]) if "employer" in l.lower()), 0)
            for x in csv.DictReader(lines[start:]):
                key = next((k for k in x if k and "employer" in k.lower() and "name" in k.lower()), None) or \
                      next((k for k in x if k and k.lower().startswith("employer")), None)
                if key:
                    names.add(sponsor_norm(x[key]))
        names.discard("")
        (out / "ca.txt").write_text("\n".join(sorted(names)))
        log(f"sponsors | CA | {len(names)}")
    except Exception as e:
        log(f"sponsors | CA | FAILED: {str(e)[:120]}")


# ---------------------------------------------------------------- certificate logs (crt.sh)
def _crt_query(q, deadline):
    """crt.sh is free but overloaded: it often answers with an error page. Retry with growing pauses."""
    for attempt, wait in enumerate((0, 20, 60, 120)):
        if time.time() + wait > deadline:
            break
        time.sleep(wait)
        try:
            r = requests.get("https://crt.sh/", params={"q": q, "output": "json", "exclude": "expired", "deduplicate": "Y"},
                             headers={"User-Agent": BROWSER_UA}, timeout=120)
            if r.status_code == 200 and r.text.lstrip().startswith("["):
                return r.json(), attempt
        except Exception:
            pass
    return None, 4


def crtsh(minutes):
    deadline = time.time() + minutes * 60
    tlds = sorted(DISC.get("tlds") or TLD_COUNTRY, key=lambda t: (t not in GULF_FIRST, t))
    found, ok, failed = [], 0, []
    for tld in tlds:
        for prefix in ("careers", "career", "jobs", "karriere", "werkenbij", "join"):
            if time.time() > deadline:
                break
            data, tries = _crt_query(f"{prefix}.%.{tld}", deadline)
            if data is None:
                failed.append(f"{prefix}.*.{tld}")
                continue
            hosts = set()
            for c in data:
                for n in str(c.get("name_value", "")).split("\n"):
                    n = n.strip().lower()
                    if n.startswith(prefix + ".") and "*" not in n and n.endswith("." + tld):
                        hosts.add(n)
            found += [dict(host=h) for h in hosts]
            ok += 1
            log(f"crt.sh | {prefix}.*.{tld} | {len(hosts)}" + (f" (after {tries} retries)" if tries else ""))
            time.sleep(5)
    if failed:
        log(f"crt.sh | no answer after retries for {len(failed)} searches: {', '.join(failed[:20])}")
    log(f"crt.sh | searches answered {ok}")
    add_sites(found, "crt.sh")


# ---------------------------------------------------------------- Common Crawl (monthly)
ATS_RANGES = [
    ("greenhouse", "io,greenhouse,"), ("lever", "co,lever,"), ("workable", "com,workable,"),
    ("ashby", "com,ashbyhq,"), ("smartrecruiters", "com,smartrecruiters,"), ("recruitee", "com,recruitee,"),
    ("bamboohr", "com,bamboohr,"), ("personio", "de,personio,"), ("personio", "com,personio,"),
    ("workday", "com,myworkdayjobs,"), ("breezy", "hr,breezy,"), ("teamtailor", "com,teamtailor,"),
    ("pinpoint", "com,pinpointhq,"),
    ("oracle", "com,oraclecloud,"), ("successfactors", "com,successfactors,"), ("successfactors", "eu,successfactors,"),
    ("taleo", "net,taleo,"), ("icims", "com,icims,"),
]
PATH_FILTER = {"oracle": "/hcmUI/CandidateExperience/%", "successfactors": "/career%", "taleo": "/careersection/%"}
CAREER_PATH = (r"^/([a-z]{2}(-[a-z]{2})?/)?(careers?|jobs?|vacanc(y|ies)|job-?openings?|join-?us|work-?with-?us|"
               r"opportunities|recruitment|karriere|stellenangebote|stellen|vacatures|werken-?bij|kariera|praca|"
               r"karrier|kariyer|carreiras?|empregos?|saiyo|recruit|wazaif)(/|\.html?|\.aspx|\.php|$)")


def _ats_from_url(platform, host, path, url=""):
    host = host.lower()
    parts = [p for p in path.split("/") if p]
    sub = host.split(".")[0]
    if platform == "oracle":
        if ".fa." in host and len(parts) > 4 and parts[3] == "sites":
            return f"{host}|{parts[4]}", sub
        return None
    if platform == "successfactors":
        m = re.search(r"[?&]company=([A-Za-z0-9_-]+)", url or "")
        return (f"{host}|{m.group(1)}", m.group(1)) if m and host.startswith("career") else None
    if platform == "taleo":
        if len(parts) > 1 and parts[0] == "careersection" and parts[1] not in ("rest", "application", "theme"):
            return f"{sub}|{parts[1]}", sub
        return None
    if platform == "icims":
        return (host, sub.replace("careers-", "")) if sub not in SKIP_SLUGS and host != "www.icims.com" else None
    if platform == "workday":
        bits = host.split(".")
        if len(bits) >= 4 and parts:
            site = parts[1] if re.fullmatch(r"[a-z]{2}-[A-Za-z]{2}", parts[0]) and len(parts) > 1 else parts[0]
            if site.lower() not in SKIP_SLUGS:
                return f"{bits[0]}/{bits[1]}/{site}", bits[0]
        return None
    if platform in ("greenhouse", "lever", "ashby", "smartrecruiters") or host == "apply.workable.com":
        if ".eu." in host or not parts:
            return None
        slug = unquote(parts[0])
        if platform == "greenhouse" and host not in ("boards.greenhouse.io", "job-boards.greenhouse.io"):
            return None
        if platform == "lever" and host != "jobs.lever.co":
            return None
        if platform == "ashby" and host != "jobs.ashbyhq.com":
            return None
        if platform == "smartrecruiters" and host not in ("careers.smartrecruiters.com", "jobs.smartrecruiters.com"):
            return None
        return (slug, slug) if slug.lower() not in SKIP_SLUGS else None
    if platform == "personio":
        return (sub, sub) if ".jobs.personio." in host and sub not in SKIP_SLUGS else None
    return (sub, sub) if sub not in SKIP_SLUGS and host.count(".") == 2 else None


def commoncrawl(minutes):
    import duckdb
    state = json.loads(STATE.read_text()) if STATE.exists() else {}
    crawl = requests.get("https://index.commoncrawl.org/collinfo.json", timeout=60).json()[0]["id"]
    if state.get("cc_crawl") == crawl:
        log(f"commoncrawl | {crawl} already done, nothing new")
        return
    paths = gzip.decompress(requests.get(f"https://data.commoncrawl.org/crawl-data/{crawl}/cc-index-table.paths.gz",
                                         timeout=120).content).decode().split()
    files = ["https://data.commoncrawl.org/" + p for p in paths if "/subset=warc/" in p]
    log(f"commoncrawl | {crawl} | {len(files)} index files")
    con = duckdb.connect()
    for s in ("INSTALL httpfs", "LOAD httpfs", "SET http_retries=10", "SET http_retry_wait_ms=3000",
              "SET http_retry_backoff=2", "SET enable_http_metadata_cache=true", "SET enable_object_cache=true",
              "SET threads=16"):
        try:
            con.execute(s)
        except Exception:
            pass
    file_list = "[" + ",".join("'" + f + "'" for f in files) + "]"
    con.execute(f"CREATE VIEW cc AS SELECT url_surtkey, url, url_host_name, url_path, fetch_status FROM read_parquet({file_list})")
    deadline = time.time() + minutes * 60
    employers = set()
    for platform, lo in ATS_RANGES:
        if time.time() > deadline:
            log("commoncrawl | time limit reached")
            break
        hi = lo[:-1] + "-"   # ',' < '-' so this covers every key that starts with lo
        try:
            pf = f" AND url_path LIKE '{PATH_FILTER[platform]}'" if platform in PATH_FILTER else ""
            sel = "url_host_name, url_path, any_value(url)" if platform == "successfactors" else "url_host_name, url_path, ''"
            got = con.execute(f"SELECT {sel} FROM cc WHERE url_surtkey >= '{lo}' "
                              f"AND url_surtkey < '{hi}' AND fetch_status = 200{pf} GROUP BY 1, 2").fetchall()
            n0 = len(employers)
            for host, path, url in got:
                hit = _ats_from_url(platform, host or "", path or "", url or "")
                if hit:
                    employers.add((platform, hit[0], hit[1]))
            log(f"commoncrawl | {platform} | urls {len(got)} | boards {len(employers) - n0}")
        except Exception as e:
            log(f"commoncrawl | {platform} | FAILED: {str(e)[:120]}")
    add_employers(employers, "commoncrawl")
    sites = []
    max_hosts = int(DISC.get("max_hosts_per_tld", 4000))
    for tld in sorted(DISC.get("tlds") or TLD_COUNTRY, key=lambda t: (t not in GULF_FIRST, t)):
        if time.time() > deadline:
            log("commoncrawl | time limit reached")
            break
        try:
            got = con.execute(
                f"SELECT url_host_name, min(url), count(*) AS n FROM cc WHERE url_surtkey >= '{tld},' AND url_surtkey < '{tld}-' "
                f"AND fetch_status = 200 AND (regexp_matches(lower(url_path), '{CAREER_PATH}') "
                f"OR regexp_matches(url_host_name, '^(careers?|jobs|karriere|recruit\\w*|join)\\.')) "
                f"GROUP BY 1 ORDER BY n DESC LIMIT {max_hosts}").fetchall()
            sites += [dict(host=h, careers_url=u) for h, u, _ in got if h]
            log(f"commoncrawl | .{tld} careers sites | {len(got)}")
        except Exception as e:
            log(f"commoncrawl | .{tld} | FAILED: {str(e)[:120]}")
    add_sites(sites, "commoncrawl")
    state["cc_crawl"] = crawl
    STATE.write_text(json.dumps(state, indent=1))


# ---------------------------------------------------------------- Web Data Commons (one time)
def wdc():
    url = "https://data.dws.informatik.uni-mannheim.de/structureddata/2023-12/quads/classspecific/JobPosting/JobPosting_domain_stats.csv"
    r = requests.get(url, timeout=600)
    r.raise_for_status()
    content = r.content
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    hosts = set()
    for line in content.decode("utf-8", "ignore").splitlines():
        for field in re.split(r"[\t,;]", line)[:3]:
            f = field.strip().strip('"').lower()
            if re.fullmatch(r"[a-z0-9.-]+\.[a-z]{2,}", f):
                hosts.add(re.sub(r"^www\.", "", f))
                break
    log(f"wdc | websites with job markup | {len(hosts)}")
    add_sites([dict(host=h) for h in sorted(hosts)], "webdatacommons")


# ---------------------------------------------------------------- employer lists
def import_open_jobs_data():
    """open-jobs-data (GitHub) employers go through the same check as everyone else: kept only with
    jobs in your markets. (Before, all 360 were read every day though few hire where you are.)"""
    try:
        data = extra._get_json(extra.OPEN_JOBS_DATA) or []
        found = set()
        for c in data:
            p = c.get("platform")
            if p == "workday" and c.get("workday"):
                w = c["workday"]
                found.add(("workday", f"{w['tenant']}/{w['shard']}/{w['site']}", c.get("name") or w["tenant"]))
            elif p in extra.FETCHERS and c.get("slug"):
                found.add((p, str(c["slug"]), c.get("name") or c["slug"]))
        add_employers(found, "open-jobs-data")
    except Exception as e:
        log(f"open-jobs-data | FAILED: {str(e)[:100]}")


TARGETS = DATA / "target_employers.csv"


def import_targets():
    """Your target employers: their websites are checked first (priority 0), so the careers system
    behind each one is found and its jobs are read daily."""
    if not TARGETS.exists():
        return
    rows = read_rows(TARGETS, ["name", "country", "sector", "website", "careers_url", "include"])
    rows = [r for r in rows if (r["include"] or "yes").strip().lower() != "no" and (r["website"] or r["careers_url"])]
    sites = []
    for r in rows:
        url = r["careers_url"] or f"https://{r['website'].strip('/')}/"
        host = urlparse(url if "://" in url else "https://" + url).netloc.lower()
        sites.append(dict(host=host, careers_url=url, priority="0", name=r["name"]))
    known = {s["host"]: s for s in read_rows(SITES, SITE_COLS)}
    # target employers already known: move them to the front and check them again
    sites_all = read_rows(SITES, SITE_COLS)
    hosts = {s["host"] for s in sites}
    for s in sites_all:
        if s["host"] in hosts and s["source"] != "target":
            s.update(priority="0", source="target", status="new" if s["status"] != "active" else s["status"])
    write_rows(SITES, SITE_COLS, sites_all)
    add_sites([s for s in sites if s["host"] not in known], "target")
    log(f"targets | {len(rows)} employers in the list")


# ---------------------------------------------------------------- entry point
def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "weekly"
    t0 = time.time()
    log(f"Discovery ({mode}) {TODAY}")
    if mode == "monthly":
        try:
            commoncrawl(float(DISC.get("cc_minutes", 150)) * SCALE)
        except Exception as e:
            log(f"commoncrawl | FAILED: {str(e)[:200]}")
    if mode == "wdc":
        try:
            wdc()
        except Exception as e:
            log(f"wdc | FAILED: {str(e)[:200]}")
    if mode == "weekly":
        sponsors()
        import_open_jobs_data()
        import_targets()
        # crt.sh (certificate logs) is not called any more: it never answers GitHub's servers, and the
        # monthly Common Crawl sweep already finds careers.* / jobs.* sites in your countries.
    probe_new(float(DISC.get("probe_minutes", 90)) * SCALE, int(DISC.get("probe_per_run", 20000)))
    validate(float(DISC.get("validate_minutes", 90)) * SCALE)
    log(f"done in {(time.time() - t0) / 60:.0f} min")
    old = LOG.read_text().splitlines()[-3000:] if LOG.exists() else []
    LOG.write_text("\n".join(old + _log) + "\n")


if __name__ == "__main__":
    main()
