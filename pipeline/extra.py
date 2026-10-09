"""Extra job sources (all free).

1. Company hiring systems with official public feeds:
   Greenhouse, Lever, Workable, Ashby, SmartRecruiters, Recruitee, BambooHR, Personio, Workday.
   Employers come from config.yaml plus the open-jobs-data list (github.com/ConorsCode/open-jobs-data).
2. Job-board sitemaps (the list of live jobs a site publishes for search engines):
   Rozee.pk and GulfTalent. Only titles that look like a fit are opened for details.
3. Optional aggregator APIs, skipped unless a free key is added as a GitHub secret:
   Careerjet (CAREERJET_AFFID), Jooble (JOOBLE_KEY), Google Jobs via SerpApi (SERPAPI_KEY).

Every source is isolated: if one fails, it is logged and the rest carry on.
Each function returns (rows, log_lines). A row has: source, title, company, location,
job_url, date_posted (YYYY-MM-DD or None), is_remote.
"""
import gzip
import html
import json
import os
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import dateparser
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

try:  # browser-like requests for job-board sites; plain requests as a fallback
    from curl_cffi import requests as cffi_requests
except Exception:  # pragma: no cover
    cffi_requests = None

UA = "Mozilla/5.0 (compatible; JobAlertsBot/1.0)"
TIMEOUT = 45
OPEN_JOBS_DATA = "https://raw.githubusercontent.com/ConorsCode/open-jobs-data/main/companies.json"


# ---------------------------------------------------------------- helpers
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, max=20), reraise=True)
def _get_json(url, **kw):
    r = requests.get(url, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=TIMEOUT, **kw)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, max=20), reraise=True)
def _post_json(url, body, headers=None):
    h = {"User-Agent": UA, "Content-Type": "application/json", "Accept": "application/json"}
    h.update(headers or {})
    r = requests.post(url, json=body, headers=h, timeout=TIMEOUT)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, max=10), reraise=True)
def _get_page(url):
    """Fetch a public web page (sitemaps, job pages) with a browser-like client when available."""
    if cffi_requests is not None:
        r = cffi_requests.get(url, impersonate="chrome", timeout=TIMEOUT, allow_redirects=True)
    else:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT, allow_redirects=True)
    if r.status_code == 404:
        return None, None
    r.raise_for_status()
    content = r.content
    if url.endswith(".gz") or content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    return content, str(r.url)


def to_date(v):
    """ISO string, epoch (s or ms), or relative text ("Posted 3 Days Ago") -> YYYY-MM-DD or None."""
    if v in (None, "", 0):
        return None
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v / 1000 if v > 1e11 else v, tz=timezone.utc).date().isoformat()
        s = str(v).strip()
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00").replace(" UTC", "+00:00")).date().isoformat()
        except ValueError:
            pass
        s = re.sub(r"(?i)\bposted\b|\+", " ", s).strip()
        d = dateparser.parse(s, settings={"PREFER_DATES_FROM": "past"})
        return d.date().isoformat() if d else None
    except Exception:
        return None


def _row(source, title, company, location, url, posted, remote=False):
    return dict(source=source, title=html.unescape(str(title or "")).strip(), company=str(company or "").strip(),
                location=str(location or ""), job_url=str(url or ""), date_posted=posted, is_remote=bool(remote))


def _join(*parts):
    return " | ".join(str(p) for p in parts if p)


# ---------------------------------------------------------------- hiring systems
def _greenhouse(slug, name):
    data = _get_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs") or {}
    return [_row("greenhouse", j.get("title"), j.get("company_name") or name,
                 _join((j.get("location") or {}).get("name"), *[o.get("name") for o in j.get("offices") or []]),
                 j.get("absolute_url"), to_date(j.get("first_published") or j.get("updated_at")))
            for j in data.get("jobs", [])]


def _lever(slug, name):
    data = _get_json(f"https://api.lever.co/v0/postings/{slug}?mode=json") or []
    out = []
    for j in data:
        c = j.get("categories") or {}
        out.append(_row("lever", j.get("text"), name, _join(c.get("location"), *(c.get("allLocations") or [])),
                        j.get("hostedUrl"), to_date(j.get("createdAt")), j.get("workplaceType") == "remote"))
    return out


def _workable(slug, name):
    data = _get_json(f"https://apply.workable.com/api/v1/widget/accounts/{slug}") or {}
    out = []
    for j in data.get("jobs", []):
        locs = [_join(j.get("city"), j.get("state"), j.get("country"))]
        locs += [_join(l.get("city"), l.get("region"), l.get("country")) for l in j.get("locations") or []]
        out.append(_row("workable", j.get("title"), data.get("name") or name, _join(*locs), j.get("url"),
                        to_date(j.get("published_on") or j.get("created_at")), j.get("telecommuting")))
    return out


def _ashby(slug, name):
    data = _get_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}") or {}
    out = []
    for j in data.get("jobs", []):
        if j.get("isListed") is False:
            continue
        locs = [j.get("location")] + [s.get("location") for s in j.get("secondaryLocations") or []]
        out.append(_row("ashby", j.get("title"), name, _join(*locs), j.get("jobUrl"), to_date(j.get("publishedAt")),
                        j.get("isRemote") or j.get("workplaceType") == "Remote"))
    return out


def _smartrecruiters(slug, name):
    out, offset = [], 0
    while offset < 1000:
        data = _get_json(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=100&offset={offset}") or {}
        items = data.get("content") or []
        for j in items:
            loc = j.get("location") or {}
            company = (j.get("company") or {}).get("name") or name
            ident = (j.get("company") or {}).get("identifier") or slug
            out.append(_row("smartrecruiters", j.get("name"), company,
                            _join(loc.get("city"), loc.get("region"), loc.get("country"), loc.get("fullLocation")),
                            f"https://jobs.smartrecruiters.com/{ident}/{j.get('id')}",
                            to_date(j.get("releasedDate")), loc.get("remote")))
        if len(items) < 100 or not data.get("totalFound"):
            break
        offset += 100
    return out


def _recruitee(slug, name):
    data = _get_json(f"https://{slug}.recruitee.com/api/offers/") or {}
    out = []
    for j in data.get("offers", []):
        locs = [j.get("location")] + [_join(l.get("city"), l.get("state"), l.get("country")) for l in j.get("locations") or []]
        out.append(_row("recruitee", j.get("title"), j.get("company_name") or name, _join(*locs), j.get("careers_url"),
                        to_date(j.get("published_at") or j.get("created_at")), j.get("remote")))
    return out


def _bamboohr(slug, name):
    data = _get_json(f"https://{slug}.bamboohr.com/careers/list") or {}
    out = []
    for j in data.get("result", []):
        loc = j.get("location") or {}
        ats = j.get("atsLocation") or {}
        out.append(_row("bamboohr", j.get("jobOpeningName"), name,
                        _join(loc.get("city"), loc.get("state"), ats.get("city"), ats.get("country")),
                        f"https://{slug}.bamboohr.com/careers/{j.get('id')}", None, j.get("isRemote")))
    return out


def _personio(slug, name):
    r = requests.get(f"https://{slug}.jobs.personio.de/xml", headers={"User-Agent": UA}, timeout=TIMEOUT)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    out = []
    for p in ET.fromstring(r.content).iter("position"):
        offices = [p.findtext("office")] + [o.text for o in p.iter("office")]
        out.append(_row("personio", p.findtext("name"), name, _join(*dict.fromkeys(o for o in offices if o)),
                        f"https://{slug}.jobs.personio.de/job/{p.findtext('id')}", to_date(p.findtext("createdAt"))))
    return out


def _workday(cfg, name, keywords, max_pages):
    tenant, site, shard = cfg["tenant"], cfg["site"], cfg["shard"]
    host = f"https://{tenant}.{shard}.myworkdayjobs.com"
    api = f"{host}/wday/cxs/{tenant}/{site}/jobs"
    seen, out = set(), []
    for kw in keywords:
        for page in range(max_pages):
            data = _post_json(api, {"appliedFacets": {}, "limit": 20, "offset": page * 20, "searchText": kw}) or {}
            posts = data.get("jobPostings") or []
            for j in posts:
                path = j.get("externalPath") or ""
                if not path or path in seen:
                    continue
                seen.add(path)
                out.append(_row("workday", j.get("title"), name, j.get("locationsText"),
                                f"{host}/en-US/{site}{path}", to_date(j.get("postedOn")),
                                "remote" in str(j.get("locationsText", "")).lower()))
            if len(posts) < 20:
                break
    return out


FETCHERS = {"greenhouse": _greenhouse, "lever": _lever, "workable": _workable, "ashby": _ashby,
            "smartrecruiters": _smartrecruiters, "recruitee": _recruitee, "bamboohr": _bamboohr,
            "personio": _personio}


def _employer_list(config, log):
    """config.yaml career_pages + (optionally) the open-jobs-data list, without duplicates."""
    boards = {}  # (platform, key) -> (name, cfg)
    cp = config.get("career_pages") or {}
    for platform in FETCHERS:
        for slug, name in (cp.get(platform) or {}).items():
            boards[(platform, str(slug).lower())] = (name, str(slug))
    for name, cfg in (cp.get("workday") or {}).items():
        boards[("workday", cfg["tenant"].lower() + "/" + cfg["site"].lower())] = (name, cfg)
    if config.get("use_open_jobs_data", True):
        try:
            extra = _get_json(OPEN_JOBS_DATA) or []
            added = 0
            for c in extra:
                p = c.get("platform")
                if p in FETCHERS and c.get("slug"):
                    key = (p, str(c["slug"]).lower())
                    if key not in boards:
                        boards[key] = (c.get("name") or c["slug"], str(c["slug"]))
                        added += 1
                elif p == "workday" and c.get("workday"):
                    w = c["workday"]
                    key = ("workday", w["tenant"].lower() + "/" + w["site"].lower())
                    if key not in boards:
                        boards[key] = (c.get("name") or w["tenant"], w)
                        added += 1
            log.append(f"open-jobs-data | employers added | {added}")
        except Exception as e:
            log.append(f"open-jobs-data | FAILED: {str(e)[:100]}")
    return boards


def hiring_systems(config):
    log, rows = [], []
    boards = _employer_list(config, log)
    wd = config.get("workday") or {}
    keywords = wd.get("keywords") or ["analyst"]
    max_pages = int(wd.get("max_pages", 2))

    def run(item):
        (platform, _), (name, cfg) = item
        try:
            got = _workday(cfg, name, keywords, max_pages) if platform == "workday" else FETCHERS[platform](cfg, name)
            return got, None
        except Exception as e:
            return [], f"{platform} | {name} | FAILED: {str(e)[:100]}"

    counts = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        for (item, (got, err)) in zip(boards.items(), pool.map(run, boards.items())):
            platform = item[0][0]
            if err:
                log.append(err)
            rows += got
            counts[platform] = counts.get(platform, 0) + len(got)
    for p, n in sorted(counts.items()):
        log.append(f"{p} | {sum(1 for k in boards if k[0] == p)} employers | {n}")
    return rows, log


# ---------------------------------------------------------------- sitemaps (Rozee.pk, GulfTalent)
def _sitemap_urls(xml_bytes):
    """Returns [(loc, lastmod)] for a urlset, or child sitemap locs for a sitemapindex."""
    root = ET.fromstring(xml_bytes)
    ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
    if root.tag.endswith("sitemapindex"):
        return [(s.findtext(f"{ns}loc"), s.findtext(f"{ns}lastmod")) for s in root.iter(f"{ns}sitemap")], True
    return [(u.findtext(f"{ns}loc"), u.findtext(f"{ns}lastmod")) for u in root.iter(f"{ns}url")], False


def _jsonld_job(page):
    """Reads the schema.org JobPosting that job pages embed for Google Jobs, if present."""
    text = page.decode("utf-8", "ignore") if isinstance(page, bytes) else page
    for block in re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', text, re.S | re.I):
        try:
            data = json.loads(html.unescape(block.strip()))
        except Exception:
            continue
        items = data if isinstance(data, list) else data.get("@graph", [data]) if isinstance(data, dict) else []
        for it in items:
            if isinstance(it, dict) and "JobPosting" in str(it.get("@type")):
                org = it.get("hiringOrganization") or {}
                locs = it.get("jobLocation") or []
                locs = locs if isinstance(locs, list) else [locs]
                where = []
                for l in locs:
                    a = (l or {}).get("address") or {}
                    if isinstance(a, dict):
                        where += [a.get("addressLocality"), a.get("addressRegion"),
                                  a.get("addressCountry") if isinstance(a.get("addressCountry"), str)
                                  else (a.get("addressCountry") or {}).get("name")]
                return dict(title=it.get("title"), company=org.get("name") if isinstance(org, dict) else org,
                            location=_join(*where), date=to_date(it.get("datePosted")),
                            remote="TELECOMMUTE" in str(it.get("jobLocationType", "")).upper())
    return None


def _page_fallback(page, city_words):
    text = page.decode("utf-8", "ignore") if isinstance(page, bytes) else page
    title = re.search(r"<title[^>]*>(.*?)</title>", text, re.S | re.I)
    plain = re.sub(r"<[^>]+>", " ", text)
    found = [c for c in city_words if re.search(rf"\b{re.escape(c)}\b", plain, re.I)]
    return dict(title=html.unescape(title.group(1)).split("|")[0].split(" - ")[0].strip() if title else None,
                company=None, location=_join(*found[:2]), date=None, remote=False)


def _slug_text(url):
    last = url.rstrip("/").split("/")[-1]
    last = re.sub(r"-jobs?-\d+$|-\d+$", "", last)
    return last.replace("-", " ")


def sitemaps(config, role_group, window_lo):
    rows, log = [], []
    sm = config.get("sitemaps") or {}
    max_details = int(sm.get("max_details_per_site", 40))
    city_words = [w for words in config["cities"].values() for w in words]

    def collect(site, sitemap_url, child_filter, url_filter):
        content, _ = _get_page(sitemap_url)
        entries, is_index = _sitemap_urls(content)
        if is_index:
            children = [loc for loc, _ in entries if loc and re.search(child_filter, loc)]
            entries = []
            for c in children:
                cc, _ = _get_page(c)
                if cc:
                    entries += _sitemap_urls(cc)[0]
        fresh = [(loc, (lm or "")[:10]) for loc, lm in entries
                 if loc and (not lm or lm[:10] >= window_lo) and url_filter(loc)]
        candidates = [(loc, lm) for loc, lm in fresh if role_group(_slug_text(loc))]
        got = 0
        for loc, lm in candidates[:max_details]:
            try:
                page, final_url = _get_page(loc)
                if not page:
                    continue
                d = _jsonld_job(page) or _page_fallback(page, city_words)
                title = d.get("title") or _slug_text(loc).title()
                rows.append(_row(site, title, d.get("company") or f"(see {site} link)",
                                 d.get("location") or "", loc, d.get("date") or lm or None, d.get("remote")))
                got += 1
            except Exception as e:
                log.append(f"{site} | detail FAILED {loc[-40:]}: {str(e)[:60]}")
        log.append(f"{site} | sitemap jobs in window {len(fresh)} | title matches {len(candidates)} | opened {got}")

    targets = []
    if sm.get("rozee", True):
        targets.append(("rozee", "https://www.rozee.pk/sitemap/jobs.xml", r".", lambda u: "-jobs-" in u))
    if sm.get("gulftalent", True):
        targets.append(("gulftalent", "https://www.gulftalent.com/sitemap.xml", r"sitemap_jx\d+\.xml",
                        lambda u: re.search(r"gulftalent\.com/(uae|saudi-arabia)/jobs/[^/]+-\d+$", u) is not None))
    for site, url, child_filter, url_filter in targets:
        try:
            collect(site, url, child_filter, url_filter)
        except Exception as e:
            log.append(f"{site} | FAILED: {str(e)[:100]}")
    return rows, log


# ---------------------------------------------------------------- optional aggregator APIs
def _public_ip():
    try:
        return requests.get("https://api.ipify.org", timeout=15).text.strip()
    except Exception:
        return "127.0.0.1"


def aggregators(config):
    rows, log = [], []
    agg = config.get("aggregators") or {}
    targets = agg.get("targets") or []          # [{"locale": "en_PK", "location": "Lahore"}, ...]
    keywords = agg.get("keywords") or []

    affid = os.environ.get("CAREERJET_AFFID", "").strip()
    if affid:
        ip, n = _public_ip(), 0
        for t in targets:
            for kw in keywords:
                try:
                    r = requests.get("http://public.api.careerjet.net/search", timeout=TIMEOUT,
                                     headers={"User-Agent": UA, "Referer": agg.get("referer_url", "https://github.com")},
                                     params=dict(locale_code=t["locale"], keywords=kw, location=t["location"],
                                                 affid=affid, user_ip=ip, user_agent=UA, sort="date", pagesize=50))
                    r.raise_for_status()
                    for j in (r.json() or {}).get("jobs", []):
                        rows.append(_row("careerjet", j.get("title"), j.get("company"),
                                         j.get("locations") or t["location"], j.get("url"), to_date(j.get("date"))))
                        n += 1
                except Exception as e:
                    log.append(f"careerjet | {t['location']} | {kw} | FAILED: {str(e)[:80]}")
        log.append(f"careerjet | {len(targets) * len(keywords)} searches | {n}")
    else:
        log.append("careerjet | skipped (no CAREERJET_AFFID secret)")

    key = os.environ.get("JOOBLE_KEY", "").strip()
    if key:
        n = 0
        for t in targets:
            for kw in keywords:
                try:
                    data = _post_json(f"https://jooble.org/api/{key}", {"keywords": kw, "location": t["location"]}) or {}
                    for j in data.get("jobs", []):
                        rows.append(_row("jooble", j.get("title"), j.get("company"),
                                         j.get("location") or t["location"], j.get("link"), to_date(j.get("updated"))))
                        n += 1
                except Exception as e:
                    log.append(f"jooble | {t['location']} | {kw} | FAILED: {str(e)[:80]}")
        log.append(f"jooble | {len(targets) * len(keywords)} searches | {n}")
    else:
        log.append("jooble | skipped (no JOOBLE_KEY secret)")

    serp = os.environ.get("SERPAPI_KEY", "").strip()
    if serp:
        n = 0
        for q in agg.get("google_jobs_queries") or []:   # kept small: free plan is 250 searches/month
            try:
                data = _get_json("https://serpapi.com/search.json",
                                 params=dict(engine="google_jobs", q=q, hl="en", api_key=serp)) or {}
                for j in data.get("jobs_results", []):
                    link = j.get("share_link") or next((a.get("link") for a in j.get("apply_options") or []), "")
                    ext = j.get("detected_extensions") or {}
                    rows.append(_row("google_jobs", j.get("title"), j.get("company_name"), j.get("location"), link,
                                     to_date(ext.get("posted_at")), ext.get("work_from_home")))
                    n += 1
            except Exception as e:
                log.append(f"google_jobs | {q} | FAILED: {str(e)[:80]}")
        log.append(f"google_jobs | {len(agg.get('google_jobs_queries') or [])} searches | {n}")
    else:
        log.append("google_jobs | skipped (no SERPAPI_KEY secret)")
    return rows, log
