"""Writes rows to Google Sheets through the Apps Script web app.

Set DRY_RUN=1 to write the payloads to ./out/ instead of calling the web app
(useful for testing without touching the real Sheets).
"""
import json
import math
import os
import time
from datetime import date, datetime
from pathlib import Path

import requests

URL = os.environ.get("APPS_SCRIPT_URL", "")
SECRET = os.environ.get("APPS_SCRIPT_SECRET", "")
DRY_RUN = os.environ.get("DRY_RUN") == "1"
BATCH = 400  # rows per request, keeps each Apps Script call well under its time limit


def _clean(v):
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    if isinstance(v, (datetime, date)):
        return v.isoformat()[:10]
    if hasattr(v, "item"):  # numpy scalars
        v = v.item()
    if isinstance(v, str) and len(v) > 45000:  # Sheets cell limit is 50k chars
        return v[:45000]
    return v


def _post(payload):
    if DRY_RUN:
        out = Path("out")
        out.mkdir(exist_ok=True)
        name = f"{payload['target']}__{payload['tab']}__{int(time.time() * 1000)}.json".replace("/", "-")
        safe = {k: v for k, v in payload.items() if k != "secret"}
        (out / name).write_text(json.dumps(safe, indent=1, ensure_ascii=False))
        return {"ok": True, "written": len(payload.get("rows", [])), "dry_run": True}

    if not URL or not SECRET:
        raise RuntimeError("APPS_SCRIPT_URL and APPS_SCRIPT_SECRET must be set")

    payload = {**payload, "secret": SECRET}
    for attempt in range(4):
        try:
            r = requests.post(URL, json=payload, timeout=180)
            data = r.json()
            if not data.get("ok"):
                raise RuntimeError(f"Apps Script error: {data}")
            return data
        except Exception as e:  # network hiccup or Apps Script busy: retry with backoff
            if attempt == 3:
                raise
            print(f"  Sheets write failed ({e}); retrying...")
            time.sleep(5 * (attempt + 1))


def write(target, tab, headers, rows, mode="append", banner=None, first=False):
    """target: 'master' or 'public'. mode: 'append' or 'replace'.

    banner: optional text placed above the header row (replace mode only).
    first: move the tab to the first position.
    """
    rows = [[_clean(v) for v in r] for r in rows]
    total = 0
    if not rows and mode == "replace":
        _post({"target": target, "tab": tab, "mode": "replace", "headers": headers,
               "rows": [], "banner": banner, "first": first})
        return 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        res = _post({
            "target": target,
            "tab": tab,
            "mode": mode if i == 0 else "append",
            "headers": headers,
            "rows": chunk,
            "banner": banner if i == 0 else None,
            "first": first and i == 0,
            "format": i + BATCH >= len(rows),  # apply formatting once, on the last batch
        })
        total += res.get("written", 0)
    return total


def send_email(subject, html, pdf_path, to=""):
    """Emails the PDF through the Apps Script. Never raises: a failed email must not fail the run.

    Works safely with an older Apps Script too: the payload is also a valid one-row write to an
    'email_log' tab, so an old script logs the attempt instead of erroring, and we report that the
    script needs updating.
    """
    import base64
    from datetime import datetime as _dt
    pdf_path = Path(pdf_path)
    payload = {
        "action": "email",
        "to": to or "",
        "subject": subject,
        "html": html,
        "filename": pdf_path.name,
        "pdf_b64": base64.b64encode(pdf_path.read_bytes()).decode(),
        # fallback fields understood by the older script (harmless log row)
        "target": "master", "tab": "email_log", "mode": "append",
        "headers": ["sent_at_utc", "subject", "attachment"],
        "rows": [[_dt.utcnow().strftime("%Y-%m-%d %H:%M"), subject, pdf_path.name]],
    }
    try:
        res = _post(payload)
    except Exception as e:
        return f"FAILED: {str(e)[:200]}"
    if res.get("dry_run"):
        return "dry run (not sent)"
    if res.get("emailed"):
        return f"sent to {res.get('to', 'script owner')}"
    return "NOT SENT: update the Apps Script to the latest Code.gs (see README) to enable emails"
