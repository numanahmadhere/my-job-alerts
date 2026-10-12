"""Readers for big-employer careers systems, all filtered to your countries where the system allows it.

- Workday: uses the site's own location filter (only jobs in your countries), then pages through them
- Oracle Recruiting Cloud (Oracle HCM "Candidate Experience"): public job feed, filtered by location
- SAP SuccessFactors: Career Site Builder RSS feed (or the job sitemap)
- Taleo: the job-search service behind the careers section page
- iCIMS: the job list pages, then job markup on each job page
- Workable: the public search across all Workable employers (by role and city)
- SmartRecruiters: country filter for large employers

Slugs (stored in data/employers.csv):
  workday         tenant/wdN/site
  oracle          host|siteNumber              e.g. eeho.fa.us2.oraclecloud.com|CX_1
  successfactors  host                         (Career Site Builder) or careerN.successfactors.com|companyId
  taleo           tenant|section               e.g. aramco|aramco_external
  icims           host                         e.g. careers-acme.icims.com
"""
import html
import json
import re
import time
import xml.etree.ElementTree as ET
from urllib.parse import quote, urljoin

import requests

from .extra import UA, _get_json, _get_page, _join, _post_json, _row, to_date
from .match import CONFIG, GEO

COUNTRY_CODES = {"Pakistan": "pk", "UAE": "ae", "Saudi Arabia": "sa", "Qatar": "qa", "Bahrain": "bh", "Oman": "om",
                 "Kuwait": "kw", "Jordan": "jo", "Egypt": "eg", "Turkey": "tr", "Malaysia": "my", "Singapore": "sg",
                 "Japan": "jp", "UK": "gb", "Ireland": "ie", "Germany": "de", "Netherlands": "nl", "Poland": "pl",
                 "Hungary": "hu", "Czechia": "cz", "Portugal": "pt", "Canada": "ca", "Australia": "au", "New Zealand": "nz"}


def in_my_markets(text):
    city, _ = GEO.place(text)
    return bool(city) and city != "Remote"


# ---------------------------------------------------------------- Workday (by country)
def _facet_choices(facets):
    """Workday facets -> {facetParameter: [(id, descriptor, count)]} for values in your markets."""
    found = {}

    def walk(node, param=None):
        if isinstance(node, list):
            for n in node:
                walk(n, param)
        elif isinstance(node, dict):
            p = node.get("facetParameter") or param
            if "id" in node and "descriptor" in node and in_my_markets(node["descriptor"]):
                found.setdefault(p, []).append((node["id"], node["descriptor"], node.get("count", 0)))
            for k in ("values", "facets"):
                if k in node:
                    walk(node[k], p)
    walk(facets)
    return found


def workday(cfg, name, keywords, max_pages=25):
    tenant, site, shard = cfg["tenant"], cfg["site"], cfg["shard"]
    host = f"https://{tenant}.{shard}.myworkdayjobs.com"
    api = f"{host}/wday/cxs/{tenant}/{site}/jobs"
    first = _post_json(api, {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""}) or {}
    choices = _facet_choices(first.get("facets") or [])
    out, seen = [], set()

    def add(posts):
        for j in posts:
            path = j.get("externalPath") or ""
            if path and path not in seen:
                seen.add(path)
                out.append(_row("workday", j.get("title"), name, j.get("locationsText"), f"{host}/en-US/{site}{path}",
                                to_date(j.get("postedOn")), "remote" in str(j.get("locationsText", "")).lower()))

    if choices:
        # the location filter with the most jobs in your markets (country-level filters usually win)
        param, vals = max(choices.items(), key=lambda kv: sum(v[2] or 0 for v in kv[1]))
        facet = {param: [v[0] for v in vals]}
        for page in range(max_pages):
            data = _post_json(api, {"appliedFacets": facet, "limit": 20, "offset": page * 20, "searchText": ""}) or {}
            posts = data.get("jobPostings") or []
            add(posts)
            if len(posts) < 20:
                break
        return out
    # no location filter on this site: search by keyword (older behaviour)
    for kw in keywords:
        for page in range(3):
            data = _post_json(api, {"appliedFacets": {}, "limit": 20, "offset": page * 20, "searchText": kw}) or {}
            posts = data.get("jobPostings") or []
            add(posts)
            if len(posts) < 20:
                break
    return out


# ---------------------------------------------------------------- SmartRecruiters (country filter)
def smartrecruiters(slug, name):
    def fetch(extra=""):
        out, offset = [], 0
        while offset < 1000:
            data = _get_json(f"https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=100&offset={offset}{extra}") or {}
            items = data.get("content") or []
            for j in items:
                loc = j.get("location") or {}
                company = (j.get("company") or {}).get("name") or name
                ident = (j.get("company") or {}).get("identifier") or slug
                out.append(_row("smartrecruiters", j.get("name"), company,
                                _join(loc.get("city"), loc.get("region"), loc.get("country"), loc.get("fullLocation")),
                                f"https://jobs.smartrecruiters.com/{ident}/{j.get('id')}", to_date(j.get("releasedDate")),
                                loc.get("remote")))
            if len(items) < 100:
                break
            offset += 100
        return out, data.get("totalFound", 0)

    rows, total = fetch()
    if total and total > 1000:   # big employer: ask for each of your countries instead
        rows = []
        for code in sorted(set(COUNTRY_CODES.values())):
            got, _ = fetch(f"&country={code}")
            rows += got
    return rows


# ---------------------------------------------------------------- Oracle Recruiting Cloud
def oracle(slug, name):
    host, site = slug.split("|")
    out, offset = [], 0
    while offset < 1000:
        url = (f"https://{host}/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true"
               f"&expand=requisitionList.secondaryLocations"
               f"&finder=findReqs;siteNumber={site},limit=100,offset={offset},sortBy=POSTING_DATES_DESC")
        data = _get_json(url) or {}
        items = (data.get("items") or [{}])[0]
        reqs = items.get("requisitionList") or []
        for j in reqs:
            locs = [j.get("PrimaryLocation"), j.get("PrimaryLocationCountry")]
            locs += [s.get("Name") for s in j.get("secondaryLocations") or [] if isinstance(s, dict)]
            out.append(_row("oracle", j.get("Title"), name, _join(*[l for l in locs if l]),
                            f"https://{host}/hcmUI/CandidateExperience/en/sites/{site}/job/{j.get('Id')}",
                            to_date(j.get("PostedDate")), str(j.get("WorkplaceTypeCode", "")).upper().endswith("REMOTE")))
        if len(reqs) < 100:
            break
        offset += 100
    return out


# ---------------------------------------------------------------- SAP SuccessFactors
def _rss_items(content):
    root = ET.fromstring(content)
    for it in root.iter("item"):
        yield {(el.tag.split("}")[-1]): (el.text or "") for el in it}


def successfactors(slug, name):
    out = []
    if "|" in slug:   # older careerN.successfactors.com sites: public job list as XML
        host, company = slug.split("|")
        page, _ = _get_page(f"https://{host}/career?company={company}&career_ns=job_listing_summary&resultType=XML")
        if page:
            for job in ET.fromstring(page).iter("Job-Listing"):
                g = lambda t: (job.findtext(t) or "").strip()
                out.append(_row("successfactors", g("JobTitle") or g("Job-Title"), name,
                                _join(g("Location"), g("City"), g("Country")),
                                f"https://{host}/career?company={company}&career_job_req_id={g('Requisition-ID') or g('ReqId')}",
                                to_date(g("Posted-Date") or g("PostedDate"))))
        return out
    host = slug
    page, _ = _get_page(f"https://{host}/services/rss/job/?locale=en_US&keywords=()")
    if page and b"<rss" in page[:500]:
        for it in _rss_items(page):
            title = html.unescape(it.get("title", ""))
            m = re.match(r"^(.*?)\s*\(([^()]*)\)\s*$", title)   # "Data Analyst (Dubai, AE, 12345)"
            t, loc = (m.group(1), m.group(2)) if m else (title, "")
            out.append(_row("successfactors", t, name, loc, it.get("link"), to_date(it.get("pubDate"))))
        return out
    # fallback: job sitemap (Career Site Builder publishes /sitemap.xml with /job/<City>-<Title>/<id>/ links)
    from .extra import _sitemap_urls, _slug_text
    sm, _ = _get_page(f"https://{host}/sitemap.xml")
    if sm:
        entries, _ = _sitemap_urls(sm)
        for loc, lm in entries:
            if loc and "/job/" in loc:
                text = _slug_text(loc.rstrip("/").rsplit("/", 1)[0])
                out.append(_row("successfactors", text.title(), name, text, loc, (lm or "")[:10] or None))
    return out


# ---------------------------------------------------------------- Taleo
def taleo(slug, name):
    tenant, section = slug.split("|")
    base = f"https://{tenant}.taleo.net/careersection"
    page, _ = _get_page(f"{base}/{section}/jobsearch.ftl?lang=en")
    portal = re.search(rb"portal=(\d+)", page or b"")
    if not portal:
        return []
    out = []
    for page_no in range(1, 11):
        body = {"multilineEnabled": False, "sortingSelection": {"sortBySelectionParam": "3", "ascendingSortingOrder": "false"},
                "fieldData": {"fields": {"KEYWORD": "", "LOCATION": ""}, "valid": True},
                "filterSelectionParam": {"searchFilterSelections": []},
                "advancedSearchFiltersSelectionParam": {"searchFilterSelections": []}, "pageNo": page_no}
        data = _post_json(f"{base}/rest/jobboard/searchjobs?lang=en&portal={portal.group(1).decode()}", body,
                          headers={"tz": "GMT+05:00"}) or {}
        reqs = data.get("requisitionList") or []
        for j in reqs:
            cols = j.get("column") or []
            title = cols[0] if cols else ""
            loc = next((c for c in cols[1:] if isinstance(c, str) and re.search(r"[A-Za-z]", c) and not re.search(r"\d{4}", c)), "")
            date = next((c for c in cols if isinstance(c, str) and re.search(r"\d{4}", c)), "")
            out.append(_row("taleo", title, name, re.sub(r"[\[\]\"]", "", str(loc)),
                            f"{base}/{section}/jobdetail.ftl?job={j.get('contestNo') or j.get('jobId')}", to_date(date)))
        if len(reqs) < 25:
            break
    return out


# ---------------------------------------------------------------- iCIMS
def icims(slug, name):
    host = slug
    out, seen = [], set()
    for pr in range(0, 10):
        page, _ = _get_page(f"https://{host}/jobs/search?ss=1&in_iframe=1&pr={pr}")
        if not page:
            break
        text = page.decode("utf-8", "ignore")
        links = re.findall(r'href="(https?://[^"]*?/jobs/(\d+)/[^"]*?/job[^"]*)"[^>]*>(.*?)</a>', text, re.S)
        new = 0
        for href, jid, label in links:
            if jid in seen:
                continue
            seen.add(jid)
            new += 1
            title = re.sub(r"<[^>]+>", " ", label)
            title = re.sub(r"\s+", " ", html.unescape(title)).strip()
            # the location is shown near the link as e.g. "AE-Dubai" or "Location ... Dubai"
            idx = text.find(href)
            near = re.sub(r"<[^>]+>", " ", text[idx: idx + 1500])
            loc = re.search(r"\b([A-Z]{2}-[A-Za-z .'-]{2,40})\b", near)
            out.append(_row("icims", title, name, loc.group(1).replace("-", ", ") if loc else "",
                            href.split("?")[0], None))
        if not new:
            break
    return out


NEW_FETCHERS = {"oracle": oracle, "successfactors": successfactors, "taleo": taleo, "icims": icims}


# ---------------------------------------------------------------- Workable search (all Workable employers)
def workable_search(config):
    roles = ["data analyst", "business intelligence", "revenue operations", "sales operations", "AI product",
             "strategy", "data scientist", "business analyst", "lecturer"]
    places = [c for m in config["markets"].values() for c in (m.get("cities") or {})]
    rows, log, n = [], [], 0
    for place in places:
        for q in roles:
            try:
                data = _get_json(f"https://jobs.workable.com/api/v1/jobs?query={quote(q)}&location={quote(place)}") or {}
                for j in data.get("jobs") or []:
                    loc = j.get("location") or {}
                    rows.append(_row("workable", j.get("title"), (j.get("company") or {}).get("title"),
                                     _join(loc.get("city"), loc.get("subregion"), loc.get("countryName")),
                                     j.get("url"), to_date(j.get("created")), j.get("workplace") == "remote"))
                    n += 1
            except Exception as e:
                log.append(f"workable search | {place} | {q} | FAILED: {str(e)[:60]}")
        time.sleep(0.5)
    log.append(f"workable search | {len(places) * len(roles)} searches | {n}")
    return rows, log


def register():
    """Make the new readers available everywhere the employer list is read."""
    from . import extra
    extra.FETCHERS.update(NEW_FETCHERS)
    extra.FETCHERS["smartrecruiters"] = smartrecruiters


register()
