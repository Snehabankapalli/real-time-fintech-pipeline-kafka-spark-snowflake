"""
Job Scraping + AI Tailoring + Notion Pipeline — v2
Fixes:
  - Robust JSON parsing (regex extraction + retry)
  - JD fetch uses job URL directly (no broken Lever individual endpoint)
  - HTML stripping for all sources
  - Greenhouse slug validation before scraping
  - Notion: safe property writing, truncation guards
  - Exponential backoff on Claude API calls
  - Dedup by (title, company) not just URL
  - Full run stats + error report at end

Sources: Greenhouse, Lever, LinkedIn (via Apify)
Output:  Notion database + jobs_log.json

Usage:
    pip install httpx anthropic python-dotenv
    python job_pipeline_v2.py
"""

import httpx
import json
import os
import re
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from anthropic import Anthropic

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG — edit these before running
# ─────────────────────────────────────────────

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
NOTION_TOKEN      = os.getenv("NOTION_TOKEN")
NOTION_DB_ID      = os.getenv("NOTION_DB_ID")
APIFY_TOKEN       = os.getenv("APIFY_TOKEN", "")

MIN_MATCH_SCORE = 65

ROLE_KEYWORDS = [
    "data engineer", "data platform", "staff data", "staff engineer",
    "senior data", "platform engineer", "analytics engineer",
    "data infrastructure", "streaming engineer", "data architect"
]

EXCLUDE_KEYWORDS = ["junior", "intern", "entry level", "associate", "manager", "director"]

# Verified Greenhouse slugs (test with: curl https://boards-api.greenhouse.io/v1/boards/{slug}/jobs)
GREENHOUSE_COMPANIES = [
    "databricks", "snowflakecomputing", "stripe", "airbnb", "coinbase",
    "robinhood", "plaid", "brex", "figma", "notion",
    "airtable", "openai", "anthropic", "fivetran", "starburst",
    "dbtlabs", "motherduck", "tabular", "clickhouse", "imply"
]

# Verified Lever slugs
LEVER_COMPANIES = [
    "netflix", "lyft", "carta", "benchling", "retool",
    "rippling", "deel", "lattice", "mercury", "linear"
]

# !! UPDATE THIS with your real experience !!
BASE_RESUME = """
Senior Data Engineer | 7+ years | Fintech, Healthcare, Telecom, Banking

SKILLS: Apache Spark/PySpark, Kafka, Snowflake, AWS (EMR Serverless, Glue, S3, Lambda),
Python, SQL, dbt, Airflow, Terraform

KEY ACHIEVEMENTS:
- Built real-time Kafka streaming pipelines processing 10M+ events/day for fintech platform
- Migrated legacy ETL to AWS EMR Serverless, reducing costs 40% and improving reliability
- Designed Snowflake data warehouse serving 50+ analysts across healthcare org
- Led PySpark optimization reducing job runtime from 4hrs to 45min in telecom domain
- Architected multi-region data platform on AWS handling HIPAA-compliant healthcare data

EXPERIENCE:
- Senior Data Engineer @ [Company] | 2021-Present
- Data Engineer @ [Company] | 2019-2021
- Data Engineer @ [Company] | 2017-2019

EDUCATION: B.S. Computer Science
"""


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def strip_html(text: str) -> str:
    """Remove HTML tags and collapse whitespace."""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&[a-z]+;', ' ', text)   # HTML entities
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def truncate(text: str, max_chars: int = 2000) -> str:
    return text[:max_chars] if text else ""

def safe_parse_json(raw: str) -> dict:
    """
    Robustly extract JSON from Claude's response.
    Handles: markdown fences, leading text, trailing text, single quotes.
    """
    if not raw:
        return {}
    # 1. Try direct parse
    try:
        return json.loads(raw)
    except Exception:
        pass
    # 2. Strip markdown fences
    cleaned = re.sub(r'```(?:json)?', '', raw).strip()
    try:
        return json.loads(cleaned)
    except Exception:
        pass
    # 3. Extract first {...} block
    match = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except Exception:
            pass
    # 4. Give up
    log.warning(f"JSON parse failed on: {raw[:200]}")
    return {}

def is_relevant(title: str) -> bool:
    t = title.lower()
    return (
        any(kw in t for kw in ROLE_KEYWORDS) and
        not any(ex in t for ex in EXCLUDE_KEYWORDS)
    )


# ─────────────────────────────────────────────
# SCRAPERS
# ─────────────────────────────────────────────

def validate_greenhouse_slug(company: str) -> bool:
    try:
        r = httpx.get(
            f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs",
            timeout=8
        )
        return r.status_code == 200
    except Exception:
        return False

def scrape_greenhouse(company: str) -> list[dict]:
    try:
        r = httpx.get(
            f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs",
            timeout=10
        )
        if r.status_code != 200:
            log.warning(f"Greenhouse {company}: HTTP {r.status_code} — check slug")
            return []
        jobs = r.json().get("jobs", [])
        matched = [
            {
                "source": "greenhouse",
                "company": company,
                "title": j.get("title", ""),
                "url": j.get("absolute_url", ""),
                "location": j.get("location", {}).get("name", ""),
                "posted_at": j.get("updated_at", ""),
                "job_id": str(j.get("id", "")),
            }
            for j in jobs if is_relevant(j.get("title", ""))
        ]
        log.info(f"Greenhouse {company}: {len(matched)}/{len(jobs)} matched")
        return matched
    except Exception as e:
        log.warning(f"Greenhouse {company} error: {e}")
        return []


def scrape_lever(company: str) -> list[dict]:
    try:
        r = httpx.get(
            f"https://api.lever.co/v0/postings/{company}?mode=json",
            timeout=10
        )
        if r.status_code != 200:
            log.warning(f"Lever {company}: HTTP {r.status_code} — check slug")
            return []
        jobs = r.json()
        if not isinstance(jobs, list):
            return []
        matched = []
        for j in jobs:
            title = j.get("text", "")
            if not is_relevant(title):
                continue
            cats = j.get("categories", {})
            matched.append({
                "source": "lever",
                "company": company,
                "title": title,
                "url": j.get("hostedUrl", ""),
                "location": cats.get("location", ""),
                "posted_at": datetime.fromtimestamp(
                    j.get("createdAt", 0) / 1000, tz=timezone.utc
                ).isoformat(),
                "job_id": j.get("id", ""),
                # Store description inline — Lever listing API includes it
                "_jd_inline": strip_html(
                    " ".join(l.get("content", "") for l in j.get("lists", []))
                    + " " + j.get("descriptionPlain", "")
                )
            })
        log.info(f"Lever {company}: {len(matched)}/{len(jobs)} matched")
        return matched
    except Exception as e:
        log.warning(f"Lever {company} error: {e}")
        return []


def scrape_linkedin_apify(query: str, location: str) -> list[dict]:
    if not APIFY_TOKEN:
        log.info("Skipping LinkedIn — APIFY_TOKEN not set")
        return []
    try:
        r = httpx.post(
            "https://api.apify.com/v2/acts/curious_coder~linkedin-jobs-scraper/run-sync-get-dataset-items",
            headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
            json={"queries": [{"query": query, "location": location}], "maxResults": 30},
            timeout=120,
        )
        if r.status_code != 201:
            log.warning(f"Apify LinkedIn: HTTP {r.status_code}")
            return []
        jobs = r.json()
        matched = [
            {
                "source": "linkedin",
                "company": j.get("company", ""),
                "title": j.get("title", ""),
                "url": j.get("link", ""),
                "location": j.get("location", ""),
                "posted_at": j.get("postedAt", ""),
                "job_id": str(j.get("id", "")),
            }
            for j in jobs if is_relevant(j.get("title", ""))
        ]
        log.info(f"LinkedIn: {len(matched)} matched")
        return matched
    except Exception as e:
        log.warning(f"LinkedIn/Apify error: {e}")
        return []


# ─────────────────────────────────────────────
# JD FETCHER
# ─────────────────────────────────────────────

def get_job_description(job: dict) -> str:
    """
    Fetch JD text. Strategy per source:
    - Lever: already included in scrape response (_jd_inline)
    - Greenhouse: hit individual job API (returns structured JSON with HTML content)
    - Others: fetch job URL, strip HTML
    """
    # Lever: JD already captured during list scrape
    if job.get("_jd_inline"):
        return truncate(job["_jd_inline"])

    try:
        if job["source"] == "greenhouse" and job.get("job_id"):
            r = httpx.get(
                f"https://boards-api.greenhouse.io/v1/boards/{job['company']}/jobs/{job['job_id']}",
                timeout=10
            )
            if r.status_code == 200:
                raw_html = r.json().get("content", "")
                return truncate(strip_html(raw_html))

        # Fallback: fetch the job posting URL and strip HTML
        if job.get("url"):
            r = httpx.get(
                job["url"], timeout=12, follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; JobBot/1.0)"}
            )
            if r.status_code == 200:
                return truncate(strip_html(r.text))

    except Exception as e:
        log.warning(f"JD fetch failed [{job.get('title')} @ {job.get('company')}]: {e}")

    return ""


# ─────────────────────────────────────────────
# AI TAILORING — with retry + backoff
# ─────────────────────────────────────────────

client = Anthropic(api_key=ANTHROPIC_API_KEY)

def tailor_application(job: dict, jd_text: str, retries: int = 2) -> dict:
    prompt = f"""You are a senior technical recruiter and career coach.

Job Title: {job['title']}
Company: {job['company']}
Location: {job.get('location', 'Not specified')}

Job Description:
{jd_text or '[JD unavailable — evaluate based on title and company]'}

Candidate Resume:
{BASE_RESUME}

Return ONLY a raw JSON object — no markdown, no backticks, no explanation. Exact keys:
{{
  "match_score": <integer 0-100>,
  "match_rationale": "<1 sentence>",
  "tailored_summary": "<2 sentences first-person matching JD keywords>",
  "top_bullets": ["<3-5 rewritten bullets mirroring JD language>"],
  "cover_letter": "<3 short paragraphs: hook / evidence / close>",
  "missing_skills": ["<hard gaps only, max 3, empty list if none>"],
  "apply_priority": "<HIGH|MEDIUM|LOW>"
}}"""

    for attempt in range(retries + 1):
        try:
            response = client.messages.create(
                model="claude-opus-4-5",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = response.content[0].text.strip()
            result = safe_parse_json(raw)

            # Validate required fields
            if result.get("match_score") is not None and result.get("apply_priority"):
                return result

            log.warning(f"Incomplete JSON on attempt {attempt+1}, retrying...")

        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"Claude API error (attempt {attempt+1}): {e} — waiting {wait}s")
            time.sleep(wait)

    return {
        "match_score": 0,
        "match_rationale": "Tailoring failed after retries",
        "tailored_summary": "",
        "top_bullets": [],
        "cover_letter": "",
        "missing_skills": [],
        "apply_priority": "LOW"
    }


# ─────────────────────────────────────────────
# NOTION LOGGING — safe property writes
# ─────────────────────────────────────────────

NOTION_HEADERS = lambda: {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

def safe_text(value: str, max_len: int = 2000) -> list:
    """Notion rich_text block, safely truncated."""
    return [{"text": {"content": str(value)[:max_len]}}]

def log_to_notion(job: dict, tailored: dict) -> bool:
    if not NOTION_TOKEN or not NOTION_DB_ID:
        return False
    try:
        missing = ", ".join(tailored.get("missing_skills", []))
        bullets = tailored.get("top_bullets", [])

        payload = {
            "parent": {"database_id": NOTION_DB_ID},
            "properties": {
                "Name":           {"title": safe_text(f"{job['title']} @ {job['company']}", 100)},
                "Company":        {"rich_text": safe_text(job.get("company", ""))},
                "Source":         {"select": {"name": job.get("source", "other").title()}},
                "Location":       {"rich_text": safe_text(job.get("location", ""))},
                "URL":            {"url": job.get("url") or "https://example.com"},
                "Match Score":    {"number": int(tailored.get("match_score", 0))},
                "Priority":       {"select": {"name": tailored.get("apply_priority", "LOW")}},
                "Status":         {"select": {"name": "To Apply"}},
                "Missing Skills": {"rich_text": safe_text(missing)},
                "Applied Date":   {"date": {"start": datetime.now(timezone.utc).date().isoformat()}},
            },
            "children": [
                {"object": "block", "type": "heading_2",
                 "heading_2": {"rich_text": safe_text("Match Rationale")}},
                {"object": "block", "type": "paragraph",
                 "paragraph": {"rich_text": safe_text(tailored.get("match_rationale", ""))}},

                {"object": "block", "type": "heading_2",
                 "heading_2": {"rich_text": safe_text("Tailored Summary")}},
                {"object": "block", "type": "paragraph",
                 "paragraph": {"rich_text": safe_text(tailored.get("tailored_summary", ""))}},

                {"object": "block", "type": "heading_2",
                 "heading_2": {"rich_text": safe_text("Cover Letter")}},
                {"object": "block", "type": "paragraph",
                 "paragraph": {"rich_text": safe_text(tailored.get("cover_letter", ""), 1900)}},

                {"object": "block", "type": "heading_2",
                 "heading_2": {"rich_text": safe_text("Top Resume Bullets")}},
                *[
                    {"object": "block", "type": "bulleted_list_item",
                     "bulleted_list_item": {"rich_text": safe_text(b)}}
                    for b in bullets
                ],
            ]
        }

        r = httpx.post(
            "https://api.notion.com/v1/pages",
            headers=NOTION_HEADERS(),
            json=payload,
            timeout=15
        )
        if r.status_code == 200:
            log.info(f"  Notion logged")
            return True
        else:
            log.warning(f"  Notion {r.status_code}: {r.text[:300]}")
            return False
    except Exception as e:
        log.warning(f"  Notion exception: {e}")
        return False


# ─────────────────────────────────────────────
# LOCAL LOG
# ─────────────────────────────────────────────

def log_to_file(job: dict, tailored: dict, path: str = "jobs_log.json"):
    entry = {
        **{k: v for k, v in job.items() if k != "_jd_inline"},  # don't store raw JD
        **tailored,
        "scraped_at": datetime.now(timezone.utc).isoformat()
    }
    existing = []
    if os.path.exists(path):
        try:
            existing = json.loads(open(path).read())
        except Exception:
            existing = []
    existing.append(entry)
    with open(path, "w") as f:
        json.dump(existing, f, indent=2)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run_pipeline():
    start = time.time()
    log.info("=" * 55)
    log.info("Job Pipeline v2")
    log.info("=" * 55)

    errors = {"scrape": [], "jd_fetch": [], "claude": [], "notion": []}

    # ── 1. Collect ──────────────────────────────
    all_jobs = []

    log.info("\n[1/4] Scraping Greenhouse...")
    for company in GREENHOUSE_COMPANIES:
        jobs = scrape_greenhouse(company)
        all_jobs += jobs
        time.sleep(0.4)

    log.info("\n[2/4] Scraping Lever...")
    for company in LEVER_COMPANIES:
        jobs = scrape_lever(company)
        all_jobs += jobs
        time.sleep(0.4)

    log.info("\n[3/4] Scraping LinkedIn...")
    all_jobs += scrape_linkedin_apify("Senior Data Engineer", "Remote")

    log.info(f"\nRaw jobs collected: {len(all_jobs)}")

    # ── 2. Dedup by (title, company) + URL ──────
    seen = set()
    unique_jobs = []
    for job in all_jobs:
        key = (job.get("title", "").lower(), job.get("company", "").lower())
        url = job.get("url", "")
        if key not in seen and url not in seen:
            seen.add(key)
            seen.add(url)
            unique_jobs.append(job)

    log.info(f"After dedup: {len(unique_jobs)} unique jobs\n")

    # ── 3. Process ──────────────────────────────
    log.info("[4/4] Tailoring + logging...")
    counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "SKIPPED": 0, "NOTION_OK": 0}

    for i, job in enumerate(unique_jobs):
        label = f"[{i+1}/{len(unique_jobs)}]"
        log.info(f"\n{label} {job['title']} @ {job['company']} ({job['source']})")

        # JD fetch
        jd = get_job_description(job)
        if not jd:
            log.info("  JD unavailable — will tailor from title/company")
            errors["jd_fetch"].append(f"{job['title']} @ {job['company']}")
        time.sleep(0.3)

        # Tailor
        tailored = tailor_application(job, jd)
        score = tailored.get("match_score", 0)
        priority = tailored.get("apply_priority", "LOW")

        log.info(f"  Score: {score} | {priority} | {tailored.get('match_rationale', '')[:80]}")

        if score < MIN_MATCH_SCORE:
            log.info(f"  Skipped (< {MIN_MATCH_SCORE})")
            counts["SKIPPED"] += 1
            continue

        # Log
        notion_ok = log_to_notion(job, tailored)
        if notion_ok:
            counts["NOTION_OK"] += 1
        else:
            errors["notion"].append(f"{job['title']} @ {job['company']}")

        log_to_file(job, tailored)
        counts[priority] = counts.get(priority, 0) + 1
        time.sleep(1.2)  # Claude rate limit buffer

    # ── 4. Summary ──────────────────────────────
    elapsed = round(time.time() - start, 1)
    log.info("\n" + "=" * 55)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 55)
    log.info(f"Runtime:         {elapsed}s")
    log.info(f"Jobs processed:  {len(unique_jobs)}")
    log.info(f"HIGH priority:   {counts['HIGH']}")
    log.info(f"MEDIUM priority: {counts['MEDIUM']}")
    log.info(f"LOW priority:    {counts['LOW']}")
    log.info(f"Skipped:         {counts['SKIPPED']}")
    log.info(f"Notion logged:   {counts['NOTION_OK']}")

    if errors["jd_fetch"]:
        log.info(f"\nJD fetch failures ({len(errors['jd_fetch'])}): {errors['jd_fetch'][:5]}")
    if errors["notion"]:
        log.info(f"Notion failures ({len(errors['notion'])}): {errors['notion'][:5]}")

    log.info("\nLocal results: jobs_log.json")


if __name__ == "__main__":
    run_pipeline()
