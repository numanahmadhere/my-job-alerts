# My Job Alerts

Daily feed of roles that fit Numan, in Lahore, Islamabad, Karachi, Dubai and Riyadh.

**Every day at 7 AM PKT** the workflow collects jobs from:
- **Job boards:** LinkedIn, Indeed, Bayt (via JobSpy)
- **Company hiring systems** (official public feeds): Greenhouse, Lever, Workable, Ashby, SmartRecruiters,
  Recruitee, BambooHR, Personio and Workday. Employers are listed in `config.yaml`, plus the
  [open-jobs-data](https://github.com/ConorsCode/open-jobs-data) list added automatically.
- **Job-board sitemaps:** Rozee.pk and GulfTalent (only titles that look like a fit are opened)
- **Remote boards:** RemoteOK, We Work Remotely
- **Optional free APIs** (skipped until you add the key as a repo secret): Careerjet (`CAREERJET_AFFID`),
  Jooble (`JOOBLE_KEY`), Google Jobs via SerpApi (`SERPAPI_KEY`)

It keeps roles whose title matches a role group in `config.yaml`
(RevOps, AI Product / Applied AI, Data & BI, Strategy & Ops, Data Science), removes duplicates,
and adds new ones to the **My jobs** tab of the Google Sheet. The last column, **Status**, is yours
(e.g. Applied, Interview, Not a fit). Daily runs only add rows, so your notes are kept.

- Change roles, cities or searches: edit `config.yaml`.
- One-time backfill: Actions > Daily job alerts > Run workflow > type 14. This rewrites the
  My jobs tab from the full history, so run it only once at the start.
- Duplicates are removed by link, by company + title + city, and by fuzzy matching
  (e.g. "Sr. Data Analyst, Systems Ltd" = "Senior Data Analyst, Systems Limited").
- What the last run found, per source: `data/last_run.txt`. Every run also adds a row to the `runs_log` tab.
