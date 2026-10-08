# My Job Alerts

Daily feed of roles that fit Numan, in Lahore, Islamabad, Karachi, Dubai and Riyadh.

**Every day at 7 AM PKT** the workflow searches LinkedIn, Indeed, Bayt and company career pages
(Greenhouse, Lever, Workable), keeps roles whose title matches a role group in `config.yaml`
(RevOps, AI Product / Applied AI, Data & BI, Strategy & Ops, Data Science), removes duplicates,
and adds new ones to the **My jobs** tab of the Google Sheet. The last column, **Status**, is yours
(e.g. Applied, Interview, Not a fit). Daily runs only add rows, so your notes are kept.

- Change roles, cities or searches: edit `config.yaml`.
- One-time backfill: Actions > Daily job alerts > Run workflow > type 14. This rewrites the
  My jobs tab from the full history, so run it only once at the start.
- What the last run found: `data/last_run.txt`. Every run also adds a row to the `runs_log` tab.
