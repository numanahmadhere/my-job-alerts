"""More free sources for the daily run.

- remote job APIs: Remotive, Himalayas, Jobicy, Working Nomads, Arbeitnow (Germany)
- "what's new" signals: Google Alerts RSS, Brave Search (past 24 h), public Telegram channels
- university job pages (HTML adverts and PDFs)
- company websites found by discovery (data/sites.csv): sitemaps, job markup (JSON-LD), RSS
- job descriptions for the visa / nationals checks

Every function returns (rows, log_lines[, extra state]) and never raises.
"""
import hashlib
import html
import io
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests

from .extra import UA, _get_json, _get_page, _join, _row, _sitemap_urls, _slug_text, to_date
from .match import CONFIG, GEO, description_from_html, plain, role_group

BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/126.0 Safari/537.36")


def url_key(url):
    return hashlib.sha1(str(url).strip().encode()).hexdigest()[:16]


def _text(page):
    return page.decode("utf-8", "ignore") if isinstance(page, (bytes, bytearray)) else str(page or "")


# ---------------------------------------------------------------- job markup (schema.org JobPosting)
def _ld_items(data):
    if isinstance(data, list):
        for d in data:
            yield from _ld_items(d)
    elif isinstance(data, dict):
        if "@graph" in data:
            yield from _ld_items(data["@graph"])
        if "itemListElement" in data:
            for el in data.get("itemListElement") or []:
                yield from _ld_items(el.get("item", el) if isinstance(el, dict) else el)
        yield data


def jsonld_jobs(page, base_url=""):
    """All JobPosting items on a page: title, company, location, date, remote, url, description, expired."""
    text = _text(page)
    out = []
    for block in re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', text, re.S | re.I):
        try:
            data = json.loads(html.unescape(block.strip()), strict=False)
        except Exception:
            continue
        for it in _ld_items(data):
            if not (isinstance(it, dict) and "JobPosting" in str(it.get("@type"))):
                continue
            org = it.get("hiringOrganization") or {}
            locs = it.get("jobLocation") or []
            locs = locs if isinstance(locs, list) else [locs]
            where = []
            for l in locs:
                a = (l or {}).get("address") if isinstance(l, dict) else None
                if isinstance(a, dict):
                    c = a.get("addressCountry")
                    where += [a.get("addressLocality"), a.get("addressRegion"),
                              c if isinstance(c, str) else (c or {}).get("name") if isinstance(c, dict) else None]
                elif isinstance(a, str):
                    where.append(a)
            arl = it.get("applicantLocationRequirements") or []
            arl = arl if isinstance(arl, list) else [arl]
            remote = "TELECOMMUTE" in str(it.get("jobLocationType", "")).upper()
            if remote:
                where.append("Remote " + " ".join(str((r or {}).get("name", "")) for r in arl if isinstance(r, dict)))
            valid = to_date(it.get("validThrough"))
            out.append(dict(title=html.unescape(str(it.get("title") or "")).strip(),
                            company=org.get("name") if isinstance(org, dict) else str(org or ""),
                            location=_join(*dict.fromkeys(str(w) for w in where if w)),
                            date=to_date(it.get("datePosted")), remote=remote,
                            url=it.get("url") or base_url,
                            description=description_from_html(it.get("description"))[:8000],
                            expired=bool(valid and valid < date.today().isoformat())))
    return out


def _fallback(page, url):
    text = _text(page)
    t = re.search(r"<title[^>]*>(.*?)</title>", text, re.S | re.I)
    title = html.unescape(t.group(1)).strip() if t else ""
    body = description_from_html(text)
    found = [w for w in GEO.city_words if re.search(rf"(?<![a-z]){re.escape(plain(w))}(?![a-z])", plain(body[:20000]))]
    return dict(title=re.split(r"\s[|\-–]\s", title)[0].strip(), company="", location=_join(*found[:2]),
                date=None, remote=False, url=url, description=body[:8000], expired=False)


def _site_name(url):
    host = urlparse(url).netloc.lower()
    host = re.sub(r"^(www|careers?|jobs|apply|karriere|join)\.", "", host)
    return host.split(".")[0].replace("-", " ").title() if host else ""


def open_job_page(url, source, fallback_title=""):
    """Open one job page; returns rows (usually one)."""
    page, final = _get_page(url)
    if not page:
        return []
    jobs = jsonld_jobs(page, final or url)
    if not jobs:
        fb = _fallback(page, final or url)
        if fallback_title and not role_group(fb["title"]):
            fb["title"] = fallback_title
        jobs = [fb]
    rows = []
    for j in jobs:
        if j.get("expired"):
            continue
        r = _row(source, j["title"], j["company"] or _site_name(final or url), j["location"],
                 j.get("url") or final or url, j["date"], j["remote"])
        r["description"] = j.get("description", "")
        rows.append(r)
    return rows


# ---------------------------------------------------------------- job descriptions for checks
def describe(url):
    """Plain-text job description for the visa / nationals checks ('' if it can't be read)."""
    u = str(url or "")
    m = re.search(r"linkedin\.com/jobs/view/(?:[^/?]*-)?(\d{6,})", u)
    try:
        if m:
            r = requests.get(f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{m.group(1)}",
                             headers={"User-Agent": BROWSER_UA, "Accept-Language": "en-US,en;q=0.9"}, timeout=30)
            if r.status_code != 200:
                return ""
            body = re.search(r'show-more-less-html__markup[^>]*>(.*?)</div>', r.text, re.S)
            crit = re.findall(r'description__job-criteria-text[^>]*>(.*?)</span>', r.text, re.S)
            return description_from_html((body.group(1) if body else r.text) + " " + " ".join(crit))[:12000]
        page, _ = _get_page(u)
        if not page:
            return ""
        jobs = jsonld_jobs(page, u)
        if jobs and jobs[0].get("description"):
            return jobs[0]["description"]
        return description_from_html(_text(page))[:12000]
    except Exception:
        return ""


# ---------------------------------------------------------------- remote job APIs
def remote_apis(config, remote_ok):
    rows, log = [], []
    wanted = set(config.get("remote_boards") or [])

    def add(name, fn):
        if name not in wanted:
            return
        try:
            got = fn()
            rows.extend(got)
            log.append(f"{name} | {len(got)}")
        except Exception as e:
            log.append(f"{name} | FAILED: {str(e)[:100]}")

    def remotive():
        out = []
        for cat in ("data", "product", "business", "sales", "all-others"):
            data = _get_json(f"https://remotive.com/api/remote-jobs?category={cat}&limit=200") or {}
            for j in data.get("jobs", []):
                loc = j.get("candidate_required_location") or "Worldwide"
                if remote_ok.search(loc) or loc.strip().lower() in ("", "anywhere"):
                    out.append(_row("remotive", j.get("title"), j.get("company_name"), f"{loc} | Remote",
                                    j.get("url"), to_date(j.get("publication_date")), True))
        return out

    def himalayas():
        out, cursor = [], None
        for _ in range(5):
            url = "https://himalayas.app/jobs/api?limit=100" + (f"&cursor={cursor}" if cursor else "")
            data = _get_json(url) or {}
            for j in data.get("jobs", []):
                restr = j.get("locationRestrictions") or []
                names = ", ".join(r if isinstance(r, str) else str((r or {}).get("name", "")) for r in restr)
                if names and not remote_ok.search(names):
                    continue
                out.append(_row("himalayas", j.get("title"), j.get("companyName"),
                                f"{names or 'Worldwide'} | Remote", j.get("applicationLink") or j.get("guid"),
                                to_date(j.get("pubDate")), True))
            cursor = data.get("nextCursor")
            if not cursor:
                break
        return out

    def jobicy():
        data = _get_json("https://jobicy.com/api/v2/remote-jobs?count=100") or {}
        return [_row("jobicy", j.get("jobTitle"), j.get("companyName"), f"{j.get('jobGeo') or 'Anywhere'} | Remote",
                     j.get("url"), to_date(j.get("pubDate")), True)
                for j in data.get("jobs", []) if remote_ok.search(str(j.get("jobGeo") or "anywhere"))]

    def workingnomads():
        data = _get_json("https://www.workingnomads.com/api/exposed_jobs/") or []
        return [_row("workingnomads", j.get("title"), j.get("company_name"), f"{j.get('location') or 'Anywhere'} | Remote",
                     j.get("url"), to_date(j.get("pub_date")), True)
                for j in data if remote_ok.search(str(j.get("location") or "anywhere"))]

    def arbeitnow():
        out = []
        for page in (1, 2, 3):
            data = _get_json(f"https://www.arbeitnow.com/api/job-board-api?page={page}") or {}
            for j in data.get("data", []):
                r = _row("arbeitnow", j.get("title"), j.get("company_name"),
                         j.get("location") + (" | Remote" if j.get("remote") else ""), j.get("url"),
                         to_date(j.get("created_at")), j.get("remote"))
                r["description"] = description_from_html(j.get("description"))[:8000]
                out.append(r)
        return out

    for name, fn in (("remotive", remotive), ("himalayas", himalayas), ("jobicy", jobicy),
                     ("workingnomads", workingnomads), ("arbeitnow", arbeitnow)):
        add(name, fn)
    return rows, log


# ---------------------------------------------------------------- Telegram public channels
def telegram(config, window_lo):
    from bs4 import BeautifulSoup
    rows, log = [], []
    for ch in (config.get("signals") or {}).get("telegram") or []:
        try:
            page, _ = _get_page(f"https://t.me/s/{ch}")
            soup = BeautifulSoup(_text(page), "html.parser")
            n = 0
            for msg in soup.select("div.tgme_widget_message"):
                body = msg.select_one(".tgme_widget_message_text")
                when = msg.select_one("time[datetime]")
                if not body:
                    continue
                posted = to_date(when["datetime"]) if when else None
                if posted and posted < window_lo:
                    continue
                text = body.get_text("\n")
                lines = [re.sub(r"^[^\w(]+", "", l).strip() for l in text.splitlines() if l.strip()]
                title = ""
                for l in lines[:8]:
                    cand = re.sub(r"(?i)^(job title|position|role|vacancy|hiring)\s*[:\-]\s*", "", l)
                    cand = re.sub(r"(?i)\s+(is hiring|hiring)\b.*$", "", cand) if role_group(cand) is None else cand
                    if role_group(cand):
                        title = cand[:150]
                        break
                if not title:
                    continue
                comp = re.search(r"(?im)^\W*(company|organi[sz]ation|employer)\s*[:\-]\s*(.+)$", text)
                at = re.search(r"\bat\s+([A-Z][\w&.\- ]{1,40})", title)
                hiring = re.search(r"^\W*([A-Z][\w&.\- ]{1,40}?)\s+is hiring", text, re.M)
                company = (comp.group(2) if comp else at.group(1) if at else hiring.group(1) if hiring else "").strip()
                locl = re.search(r"(?im)^\W*(location|city|based in)\s*[:\-]\s*(.+)$", text)
                location = locl.group(2) if locl else _join(*[w for w in GEO.city_words
                                                             if re.search(rf"(?<![a-z]){re.escape(plain(w))}(?![a-z])", plain(text))][:2])
                if re.search(r"(?i)\bremote\b", text) and not location:
                    location = "Remote"
                links = [a["href"] for a in body.select("a[href]") if "t.me/" not in a["href"]]
                post = msg.get("data-post")
                link = links[0] if links else (f"https://t.me/{post}" if post else f"https://t.me/s/{ch}")
                r = _row("telegram", re.sub(r"\s+at\s+[A-Z].*$", "", title), company or f"(see Telegram: {ch})",
                         location, link, posted)
                r["description"] = text[:8000]
                rows.append(r)
                n += 1
            log.append(f"telegram | {ch} | {n}")
        except Exception as e:
            log.append(f"telegram | {ch} | FAILED: {str(e)[:80]}")
    return rows, log


# ---------------------------------------------------------------- Google Alerts + Brave (past 24 h)
def _alert_feeds(config):
    feeds = list((config.get("signals") or {}).get("google_alerts") or [])
    feeds += [f.strip() for f in re.split(r"[\n,]", os.environ.get("GOOGLE_ALERTS_FEEDS", "")) if f.strip()]
    return list(dict.fromkeys(feeds))


def _real_link(link):
    q = parse_qs(urlparse(link).query)
    return unquote(q["url"][0]) if "url" in q else link


def candidates_alerts(config, window_lo):
    import feedparser
    cands, log = [], []
    feeds = _alert_feeds(config)
    if not feeds:
        return cands, ["google_alerts | skipped (no feeds yet: add GOOGLE_ALERTS_FEEDS secret)"]
    for f in feeds:
        try:
            r = requests.get(f, headers={"User-Agent": UA}, timeout=45)
            d = feedparser.parse(r.content)
            n = 0
            for e in d.entries:
                posted = to_date(e.get("published") or e.get("updated"))
                if posted and posted < window_lo:
                    continue
                title = description_from_html(e.get("title", ""))
                cands.append(dict(url=_real_link(e.get("link", "")), title=title, source="google_alerts",
                                  snippet=description_from_html(e.get("content", [{}])[0].get("value", "")
                                                                if e.get("content") else e.get("summary", ""))))
                n += 1
            log.append(f"google_alerts | feed {f[-12:]} | {n}")
        except Exception as e:
            log.append(f"google_alerts | FAILED: {str(e)[:80]}")
    return cands, log


def _rotation(items, per_day, today):
    if not items:
        return []
    start = (today.toordinal() * per_day) % len(items)
    return [items[(start + i) % len(items)] for i in range(min(per_day, len(items)))]


def candidates_brave(config, today):
    key = os.environ.get("BRAVE_KEY", "").strip()
    if not key:
        return [], ["brave | skipped (no BRAVE_KEY secret)"]
    sig = config.get("signals") or {}
    combos = [f"{r} {p} (job OR vacancy OR careers OR hiring)" for p in sig.get("brave_places", [])
              for r in sig.get("brave_roles", [])]
    cands, log, n = [], [], 0
    for q in _rotation(combos, int(sig.get("brave_queries_per_day", 30)), today):
        try:
            r = requests.get("https://api.search.brave.com/res/v1/web/search", timeout=30,
                             headers={"X-Subscription-Token": key, "Accept": "application/json"},
                             params=dict(q=q, freshness="pd", count=20))
            r.raise_for_status()
            for res in (r.json().get("web") or {}).get("results", []):
                cands.append(dict(url=res.get("url"), title=description_from_html(res.get("title")),
                                  source="brave", snippet=description_from_html(res.get("description"))))
                n += 1
            time.sleep(1.1)   # free plan: 1 request per second
        except Exception as e:
            log.append(f"brave | {q[:40]} | FAILED: {str(e)[:80]}")
    log.append(f"brave | results {n}")
    return cands, log


def open_candidates(cands, seen, budget):
    """Open candidate links whose title looks like a fit; read the job markup on the page."""
    rows, log, opened = [], [], []
    todo = []
    for c in cands:
        u = c.get("url") or ""
        if not u.startswith("http") or url_key(u) in seen:
            continue
        title = re.split(r"\s[|\-–]\s", c.get("title") or "")[0]
        if not (role_group(c.get("title")) or role_group(title)):
            continue
        todo.append((u, title, c["source"]))
    todo = list(dict.fromkeys(todo))[:budget]

    def go(item):
        u, title, src = item
        try:
            return open_job_page(u, src, title), None
        except Exception as e:
            return [], f"{src} | page FAILED {u[-40:]}: {str(e)[:60]}"

    with ThreadPoolExecutor(6) as pool:
        for (u, _, _), (got, err) in zip(todo, pool.map(go, todo)):
            opened.append(url_key(u))
            rows += got
            if err:
                log.append(err)
    log.append(f"signals | pages opened {len(todo)} | jobs read {len(rows)}")
    return rows, log, opened


# ---------------------------------------------------------------- universities
TEACH = re.compile(r"\b(lecturer|assistant professor|associate professor|professor|faculty|instructor|teaching)\b", re.I)


def universities(config, seen):
    from .match import GROUPS
    req = next((g[2] for g in GROUPS if g[0] == "Teaching & Faculty"), None)
    subj_rx = re.compile(r"\b(data science|data analytics|business analytics|analytics|artificial intelligence|"
                         r"machine learning|computer science|information (systems|technology)|statistics|business|"
                         r"management|economics|finance|marketing|mba|bba)\b", re.I)
    link_rx = re.compile(r"(?i)(job|career|vacanc|position|faculty|advert|recruit|opening|\.pdf$)")
    rows, log, opened = [], [], []
    for u in config.get("universities") or []:
        try:
            page, final = _get_page(u["url"])
            if not page:
                log.append(f"university | {u['name']} | page not found")
                continue
            text = _text(page)
            links = []
            for href, label in re.findall(r'<a[^>]+href=["\']([^"\'#]+)["\'][^>]*>(.*?)</a>', text, re.S | re.I):
                label = description_from_html(label)
                full = urljoin(final or u["url"], html.unescape(href))
                if not full.startswith("http"):
                    continue
                same = urlparse(full).netloc.endswith(urlparse(u["url"]).netloc.replace("www.", ""))
                if (same or full.lower().endswith(".pdf")) and (link_rx.search(full) or link_rx.search(label)):
                    links.append((full, label))
            links = [l for l in dict.fromkeys(links) if url_key(l[0]) not in seen][:15]
            n = 0
            for link, label in links:
                opened.append(url_key(link))
                try:
                    doc, _ = _get_page(link)
                    if not doc:
                        continue
                    if link.lower().endswith(".pdf") or doc[:4] == b"%PDF":
                        from pypdf import PdfReader
                        body = " ".join((p.extract_text() or "") for p in PdfReader(io.BytesIO(doc)).pages[:6])
                    else:
                        body = description_from_html(_text(doc))
                    if not TEACH.search(body + " " + label):
                        continue
                    subjects = list(dict.fromkeys(m.group(0).title() for m in subj_rx.finditer(body)))[:4]
                    title = f"Faculty / Lecturer roles: {', '.join(subjects)}" if subjects else ""
                    if not title or (req and not req.search(title)):
                        continue
                    city = GEO.place(label)[0] or GEO.place(body[:3000])[0] or u.get("city")
                    if city in (None, "Remote") or "(city not stated)" in str(city):
                        city = u.get("city")
                    r = _row("university", title, u["name"], city, link, date.today().isoformat())
                    r["description"] = body[:8000]
                    rows.append(r)
                    n += 1
                except Exception as e:
                    log.append(f"university | {u['name']} | advert FAILED: {str(e)[:60]}")
            log.append(f"university | {u['name']} | new links {len(links)} | relevant {n}")
        except Exception as e:
            log.append(f"university | {u['name']} | FAILED: {str(e)[:80]}")
    return rows, log, opened


# ---------------------------------------------------------------- company websites (data/sites.csv)
def _conditional_get(url, st):
    """GET with If-None-Match / If-Modified-Since. Returns (content or None if unchanged, new_state)."""
    headers = {"User-Agent": BROWSER_UA}
    if st.get("etag"):
        headers["If-None-Match"] = st["etag"]
    if st.get("last_modified"):
        headers["If-Modified-Since"] = st["last_modified"]
    try:
        from curl_cffi import requests as cr
        r = cr.get(url, headers=headers, impersonate="chrome", timeout=45, allow_redirects=True)
    except Exception:
        r = requests.get(url, headers=headers, timeout=45, allow_redirects=True)
    new = dict(st)
    if r.status_code == 304:
        return None, new
    r.raise_for_status()
    content = r.content
    if content[:2] == b"\x1f\x8b":
        import gzip
        content = gzip.decompress(content)
    h = hashlib.sha1(content).hexdigest()[:16]
    new.update(etag=r.headers.get("ETag", ""), last_modified=r.headers.get("Last-Modified", ""))
    if h == st.get("hash"):
        return None, new
    new["hash"] = h
    return content, new


def read_site(site, st, seen, window_lo, pages_per_site):
    """One company website. Returns (rows, opened_keys, new_state, log_line)."""
    rows, opened = [], []
    name = site.get("name") or _site_name(site.get("careers_url") or site["host"])
    kind = site.get("kind", "")
    new_state = dict(st)
    found = 0
    if kind == "sitemap" and site.get("sitemap_url"):
        content, new_state = _conditional_get(site["sitemap_url"], st)
        if content is None:
            return rows, opened, new_state, "unchanged"
        entries, is_index = _sitemap_urls(content)
        if is_index:   # take the job-like child sitemaps only
            kids = [loc for loc, _ in entries if loc and re.search(r"(?i)job|career|vacanc|position|stellen|vacature", loc)][:5]
            entries = []
            for k in kids:
                kc, _ = _get_page(k)
                if kc:
                    entries += _sitemap_urls(kc)[0]
        job_rx = re.compile(site.get("job_url_pattern") or r"(?i)/(jobs?|careers?|vacanc\w*|positions?|openings?|stellen\w*|vacatures?)/")
        fresh = [(loc, (lm or "")[:10]) for loc, lm in entries
                 if loc and job_rx.search(loc) and (not lm or lm[:10] >= window_lo) and url_key(loc) not in seen]
        # open titles that look like a fit first; then pages whose address says nothing about the title
        fit = [x for x in fresh if role_group(_slug_text(x[0]))]
        blind = [x for x in fresh if not re.search(r"[a-z]{4,}", _slug_text(x[0]))]
        for loc, lm in (fit + blind)[:pages_per_site]:
            opened.append(url_key(loc))
            try:
                for r in open_job_page(loc, "site"):
                    r["date_posted"] = r["date_posted"] or lm or None
                    r["company"] = r["company"] or name
                    r["location"] = r["location"] or site.get("default_location", "")
                    rows.append(r)
            except Exception:
                pass
        found = len(fresh)
    elif kind == "rss" and site.get("rss_url"):
        import feedparser
        content, new_state = _conditional_get(site["rss_url"], st)
        if content is None:
            return rows, opened, new_state, "unchanged"
        for e in feedparser.parse(content).entries:
            link = e.get("link", "")
            if not link or url_key(link) in seen or not role_group(e.get("title", "")):
                continue
            opened.append(url_key(link))
            summary = description_from_html(e.get("summary", ""))
            loc = _join(*[w for w in GEO.city_words if re.search(rf"(?<![a-z]){re.escape(plain(w))}(?![a-z])",
                                                                 plain(e.get("title", "") + " " + summary))][:2])
            r = _row("site", e.get("title"), name, loc or site.get("default_location", ""), link,
                     to_date(e.get("published") or e.get("updated")))
            r["description"] = summary[:8000]
            rows.append(r)
            found += 1
    elif site.get("careers_url"):   # a careers page with job markup or job links
        content, new_state = _conditional_get(site["careers_url"], st)
        if content is None:
            return rows, opened, new_state, "unchanged"
        for j in jsonld_jobs(content, site["careers_url"]):
            if j["expired"] or not role_group(j["title"]):
                continue
            r = _row("site", j["title"], j["company"] or name, j["location"], j["url"], j["date"], j["remote"])
            r["description"] = j["description"]
            rows.append(r)
            found += 1
        text = _text(content)
        links = []
        for href, label in re.findall(r'<a[^>]+href=["\']([^"\'#]+)["\'][^>]*>(.*?)</a>', text, re.S | re.I):
            label = description_from_html(label)
            if role_group(label):
                links.append((urljoin(site["careers_url"], html.unescape(href)), label))
        for link, label in [l for l in dict.fromkeys(links) if url_key(l[0]) not in seen][:pages_per_site]:
            opened.append(url_key(link))
            try:
                got = open_job_page(link, "site", label)
                for r in got:
                    r["company"] = r["company"] or name
                    if not r["location"]:
                        r["location"] = site.get("default_location", "")
                rows += got
                found += 1
            except Exception:
                pass
    new_state["last_checked"] = date.today().isoformat()
    new_state["last_found"] = str(found)
    return rows, opened, new_state, f"read {found}"


def site_reader(sites, state, seen, window_lo, pages_per_site, deadline):
    """sites: list of dicts (this runner's share). state: host -> dict. Returns rows, log, opened, state_updates."""
    rows, opened, updates, stats = [], [], {}, {"read": 0, "unchanged": 0, "failed": 0, "jobs": 0}

    def go(site):
        if time.time() > deadline:
            return [], [], None, "skipped"
        try:
            return read_site(site, state.get(site["host"], {}), seen, window_lo, pages_per_site)
        except Exception as e:
            st = dict(state.get(site["host"], {}))
            st["last_checked"] = date.today().isoformat()
            st["fails"] = str(int(st.get("fails") or 0) + 1)
            return [], [], st, "failed"

    with ThreadPoolExecutor(10) as pool:
        for site, (got, op, st, what) in zip(sites, pool.map(go, sites)):
            rows += got
            opened += op
            if st is not None:
                updates[site["host"]] = st
            key = "failed" if what == "failed" else "unchanged" if what == "unchanged" else "read" if what != "skipped" else None
            if key:
                stats[key] += 1
    stats["jobs"] = len(rows)
    return rows, [f"company sites | {json.dumps(stats)}"], opened, updates
