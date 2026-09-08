# Ingestion Core — Design Spec

Sub-project 1 of the Autonomous Job Ingestion & Application Agent (see
`Documentation.txt` for the full system vision). This slice delivers a working
end-to-end pipeline from job sources to a Telegram alert. It deliberately
excludes Claude fit-scoring/CV-tailoring, PDF compilation, auto-apply, and the
dashboard/setup wizard — those are separate sub-projects with their own specs.

## Scope

In scope:
- Core data model (SQLite schema, `master_profile.json`, `blacklist.json`,
  `config/sources.json`)
- Six ingestion adapters behind a common interface: We Work Remotely,
  RemoteOK, Remotive, NoDesk, FlexJobs, LinkedIn
- Source health monitoring & rate limiting
- Blacklist filtering
- Tech-stack keyword pre-filtering
- De-duplication (exact per-source, fuzzy cross-source)
- Telegram notifier with `Snooze 24h` / `Skip` actions
- Full-time / part-time employment-type preference, respected as a filter

Out of scope (future sub-projects): Claude fit scoring & CV tailoring, PDF
compilation, Playwright auto-apply, dashboard & setup wizard, weekly digest,
calendar sync.

## Architecture

```
[6 Adapters: WWR, RemoteOK, Remotive, NoDesk, FlexJobs, LinkedIn]
       (fetch gated by Source Health & Rate Limiter cooldown/delay)
                |
                v
        [Seen-Job Check]           (exact source_job_id; drop reposts from
                                     the same source before any other work)
                |
                v
        [Blacklist Filter]         (drop by company/keyword)
                |
                v
   [Tech-Stack Keyword Pre-Filter] (keep only jobs matching configured keywords;
                                     also drops jobs whose employment_type doesn't
                                     match accepting_full_time/accepting_part_time)
                |
                v
   [Fuzzy Cross-Source Dedup]      (title/company similarity against recent jobs)
                |
                v
        [Telegram Notifier]        (job details + [Snooze 24h] [Skip] buttons)
```

Cheap filters (seen check, blacklist, keyword/employment-type match) run
before the costlier fuzzy-dedup check, so fuzzy matching only runs on jobs
that would otherwise be alerted.

Every job an adapter returns is recorded in `seen_jobs` regardless of filter
outcome, so a source is never re-processed on the next poll. Only jobs that
pass blacklist + pre-filter are written to the full `jobs` table.

## Data Model

### SQLite schema (`data/app_database.db`)

```sql
CREATE TABLE seen_jobs (
    source TEXT NOT NULL,
    source_job_id TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    PRIMARY KEY (source, source_job_id)
);

CREATE TABLE jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_job_id TEXT NOT NULL,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    description TEXT,
    url TEXT NOT NULL,
    location TEXT,
    employment_type TEXT NOT NULL,   -- 'full_time' | 'part_time' | 'contract' | 'unknown'
    salary_raw TEXT,
    posted_at TEXT,
    fetched_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',  -- 'new' | 'alerted' | 'snoozed' | 'skipped'
    UNIQUE (source, source_job_id)
);

CREATE TABLE source_health (
    source TEXT PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_success_at TEXT,
    last_failure_at TEXT,
    disabled INTEGER NOT NULL DEFAULT 0,
    disabled_at TEXT
);
```

### `data/master_profile.json`

```json
{
  "name": "string",
  "tech_stack_keywords": ["python", "react", "..."],
  "employment_preferences": {
    "accepting_full_time": true,
    "accepting_part_time": false
  }
}
```

`employment_preferences` is the FT/PT toggle: hand-edited for this
sub-project, replaced by a Dashboard control in a later sub-project.

### `config/blacklist.json`

```json
{
  "companies": ["Acme Recruiting Inc"],
  "keywords": ["unpaid", "commission-only"]
}
```

### `config/sources.json`

```json
{
  "we_work_remotely": {"enabled": true, "poll_interval_minutes": 30, "delay_range_seconds": [1, 4]},
  "remoteok":         {"enabled": true, "poll_interval_minutes": 30, "delay_range_seconds": [1, 4]},
  "remotive":         {"enabled": true, "poll_interval_minutes": 30, "delay_range_seconds": [1, 4]},
  "nodesk":           {"enabled": true, "poll_interval_minutes": 60, "delay_range_seconds": [2, 6]},
  "flexjobs":         {"enabled": true, "poll_interval_minutes": 60, "delay_range_seconds": [2, 6]},
  "linkedin":         {"enabled": true, "poll_interval_minutes": 120, "delay_range_seconds": [5, 15]}
}
```

## Ingestion Adapters

Common interface:

```python
@dataclass
class JobPosting:
    source: str
    source_job_id: str
    title: str
    company: str
    description: str
    url: str
    location: str
    employment_type: str   # normalized to 'full_time' | 'part_time' | 'contract' | 'unknown'
    salary_raw: str | None
    posted_at: datetime | None

class SourceAdapter(ABC):
    name: str
    def fetch(self) -> list[JobPosting]: ...
```

| Adapter | Method | Notes |
|---|---|---|
| We Work Remotely | RSS feed parse (`feedparser`) | Public, no auth |
| RemoteOK | Public JSON API (`requests`) | Public, no auth |
| Remotive | Public JSON API (`requests`) | Public, no auth |
| NoDesk | HTML scrape (`requests` + BeautifulSoup) | No public feed found; higher breakage risk on layout change |
| FlexJobs | HTML scrape, may require a logged-in session for full listings | Gated content; treat as higher risk |
| LinkedIn | Authenticated Playwright session (`storageState.json` cookies) | Highest ban/breakage risk; relies most heavily on Source Health to contain damage |

Each adapter lives in its own file under `src/ingestion/`, implementing only
`fetch()`. Adding a seventh source later means adding one file, not touching
the pipeline.

## Source Health & Rate Limiting

- Before each `fetch()` call, the orchestrator waits a random delay drawn
  from that source's `delay_range_seconds`.
- A source is polled only when `poll_interval_minutes` has elapsed since its
  last attempt (tracked via `source_health.last_success_at` /
  `last_failure_at`).
- On adapter exception: increment `consecutive_failures`, record
  `last_failure_at`. At 5 consecutive failures (fixed threshold for this
  build), set `disabled = 1`, record `disabled_at`, and send one Telegram
  alert naming the source and last error. A disabled source is skipped by
  the orchestrator until manually re-enabled by editing `sources.json`.
- On successful fetch: reset `consecutive_failures` to 0, record
  `last_success_at`.

## Filtering Pipeline

1. **Dedup (seen check):** if `(source, source_job_id)` already in
   `seen_jobs`, discard silently. Otherwise insert into `seen_jobs` and
   continue.
2. **Blacklist:** discard if `company` case-insensitively matches
   `blacklist.json.companies`, or if `title`/`description` contains any
   `blacklist.json.keywords`.
3. **Employment-type filter:** discard if the job's `employment_type` is
   `full_time` and `accepting_full_time` is `false`, or `part_time` and
   `accepting_part_time` is `false`. Jobs with `employment_type: unknown` or
   `contract` always pass this check (only explicit full/part-time
   preferences gate).
4. **Tech-stack keyword filter:** discard unless `title` or `description`
   contains at least one of `master_profile.json.tech_stack_keywords`
   (case-insensitive substring match).
5. **Fuzzy cross-source dedup:** compare against `jobs` rows from the last 14
   days using normalized-title + normalized-company similarity (e.g.
   `rapidfuzz` token-sort ratio ≥ 90). If matched, discard as a repost.
6. Surviving jobs are inserted into `jobs` with `status = 'new'`, then
   alerted and updated to `status = 'alerted'`.

## Telegram Notifier

Alert message includes: title, company, location, employment type, source,
and URL. Two inline buttons:
- `Snooze 24h` — sets `status = 'snoozed'`; re-alerted by the orchestrator
  24 hours later (checked each poll cycle).
- `Skip` — sets `status = 'skipped'`.

`Auto-Apply Now` and `Get PDF & Draft` are not implemented in this
sub-project (no Claude/compiler/applier yet) and are omitted from the
keyboard rather than shown disabled.

A separate, unconditional alert type is used for the Source Health failure
notification described above.

## Orchestration

`main.py` runs a polling loop: each cycle, for every source in
`sources.json` where `enabled` is true, `disabled` is false, and the poll
interval has elapsed, call its adapter's `fetch()` (after the randomized
delay) and run results through the filtering pipeline. The loop then sleeps
briefly (e.g. 60s) before re-checking which sources are due. This can later
be replaced by Windows Task Scheduler invoking a `--once` mode instead of a
long-running process, without changing adapter or pipeline code.

## Error Handling

- Each adapter call is wrapped individually — one adapter raising never
  stops the cycle for other sources.
- Filter-stage errors (e.g. a malformed job record) fail closed: the job is
  logged and dropped, never alerted, rather than crashing the pipeline.
- All exceptions are logged with source name, stage, and message to a local
  log file for later inspection.

## Testing

- Each adapter tested against recorded fixture responses (saved HTML/JSON),
  never live network calls, asserting correct `JobPosting` parsing.
- Unit tests for blacklist filter, employment-type filter, and keyword
  filter logic (pass/fail cases each).
- Unit tests for fuzzy-dedup matching, including a threshold case (near-miss
  titles that should and shouldn't match).
- One integration test: a fixture job fed through the full pipeline with a
  stubbed Telegram sender, asserting the sender was called with expected
  content and the DB ends in the expected state.
