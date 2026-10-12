# My Job Alerts

Daily feed of roles that fit Numan, in 51 cities across 24 countries plus remote roles open to Pakistan.
Everything is free. Settings live in `config.yaml`; no code changes are needed to add a city, a role or a search.

## Where
- **Pakistan:** Lahore, Islamabad / Rawalpindi, Karachi
- **Gulf:** Dubai, Abu Dhabi, Sharjah, Ajman, Ras Al Khaimah, Al Ain, Riyadh, Jeddah, Dammam, Al Khobar, Dhahran,
  Mecca, Medina, Doha, Manama, Muscat, Salalah, Kuwait City
- **Middle East:** Amman, Cairo, Giza, Alexandria, Istanbul
- **Asia-Pacific:** Kuala Lumpur, Singapore, Tokyo, Sydney, Melbourne, Auckland
- **Europe:** London, Manchester, Birmingham, Dublin, Berlin, Munich, Frankfurt, Hamburg, Amsterdam, Rotterdam,
  The Hague, Warsaw, Kraków, Budapest, Prague, Lisbon
- **Canada:** Toronto, Vancouver, Montreal
- **Remote:** only roles open worldwide / Asia / EMEA / Middle East / Pakistan (US-only remote roles are dropped)

A job that names only the country (e.g. "Qatar") is kept as "Qatar (city not stated)". Arabic city names and
accented spellings are matched.

## Visa rules
- **Gulf, Jordan, Egypt:** "nationals only" roles (Saudization, Emiratisation, "UAE nationals only", ...) are dropped.
- **Malaysia, Singapore, Turkey:** kept unless the ad says citizens / locals only or "no sponsorship".
- **UK, Ireland, Germany, Netherlands, Poland, Hungary, Czechia, Portugal, Canada, Australia, New Zealand, Japan:**
  kept only if the ad mentions visa sponsorship or relocation, or (UK, Netherlands, Canada) the employer is on
  the government list of licensed visa sponsors. The reason appears in "Why it matches".

## Sources
**Daily (7 AM PKT):**
- LinkedIn, Indeed (22 countries), via JobSpy (Bayt was removed: it blocks GitHub's servers)
- Company hiring systems (official public job feeds): Greenhouse, Lever, Workable, Ashby, SmartRecruiters,
  Recruitee, BambooHR, Personio, Workday, Breezy, Teamtailor, Pinpoint, and for big employers Oracle Recruiting
  Cloud, SAP SuccessFactors, Taleo and iCIMS. Workday and SmartRecruiters are searched inside your countries only.
  Workable is also searched across all its employers by role and city.
- Target employers (`data/target_employers.csv`): about 160 Pakistan and Gulf employers. Weekly discovery opens
  each one's website, finds its careers system and adds it. Edit the list (set include to no to drop one). Employers come from `config.yaml`,
  the [open-jobs-data](https://github.com/ConorsCode/open-jobs-data) list, and `data/employers.csv`
  (found automatically, see Discovery)
- Job-board sitemaps: Rozee.pk, GulfTalent (all Gulf countries, Jordan, Egypt)
- Remote boards: RemoteOK, We Work Remotely, Remotive, Himalayas, Jobicy, Working Nomads, Arbeitnow
- Company websites found by discovery (`data/sites.csv`): sitemaps, job markup, RSS, careers pages.
  Each site is re-read only when it changed (ETag / content fingerprint).
- University job pages (Pakistan + Gulf): new adverts and PDFs are read and kept when they mention a teaching
  role in a subject that fits
- Public Telegram channels (list in `config.yaml`)
- Optional, each skipped until its secret is added: Google Alerts (`GOOGLE_ALERTS_FEEDS`), Brave Search past 24 h
  (`BRAVE_KEY`), Google Jobs via SerpApi (`SERPAPI_KEY`), Careerjet (`CAREERJET_AFFID`), Jooble (`JOOBLE_KEY`)

**Discovery (finds new employers and sites to read):**
- Weekly (Sundays): UK / Netherlands / Canada visa-sponsor lists, certificate logs for new `careers.*` sites in
  your countries, checks new websites, checks employers' job boards (kept only if they have jobs in your markets)
- Monthly (3rd): Common Crawl sweep for hiring-system job boards and careers pages on your countries' web addresses
- One time (run by hand): Web Data Commons list of websites that publish job markup

## How the daily run works
Two groups of GitHub runners work at the same time: 10 search runners (LinkedIn, Indeed, hiring systems, feeds)
and 4 website runners (company websites found by discovery). Each runner has its own internet address, which
keeps LinkedIn from blocking; a blocked LinkedIn search is retried once at the end. Matching jobs listed without
a location get it from the job page. A final step combines everything, removes duplicates (by link, company + title +
city, and fuzzy matching), checks visa / nationals rules, and **adds** new jobs to the Sheet.

## The Sheet
- **My jobs:** new jobs are added below the last row. Existing rows, and anything you typed in them, are never
  changed. Columns: Date found, Posted, Role, Company, City, Why it matches, Level, Found on, Apply link,
  Status (yours), Region.
- **Companies newly hiring:** employers seen for the first time, one row per company per day.
- **Visa unclear:** UK / Europe / Canada / Australia / Japan roles whose ad says nothing about visas.
- **Jobs by source:** new jobs per source each run.
- **Run health:** one row per run: jobs scanned, searches done vs planned, failures, coverage %.

## Notes
- "Run workflow" has an optional "look further back" box. It only adds jobs; it never rewrites the tab.
- Private repository: GitHub gives 2,000 free minutes a month, so the run shrinks to fit (2 search runners and 1 website runner, about
  15 minutes each; searches not reached rotate day by day) and discovery uses a fifth of its time.
  Public repository: unlimited free minutes, full run.
- What the last run found, per source: `data/last_run.txt`. Discovery: `data/discovery_log.txt`.
