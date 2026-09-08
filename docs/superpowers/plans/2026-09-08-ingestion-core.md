# Ingestion Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the end-to-end pipeline from six job sources through filtering/dedup to a Telegram alert with Snooze/Skip actions.

**Architecture:** Six independent source adapters implement a common `fetch() -> list[JobPosting]` interface. An orchestrator runs each fetched posting through seen-check → blacklist → employment-type filter → tech-stack keyword filter → fuzzy dedup, inserting survivors into SQLite and sending a Telegram alert. A polling loop in `main.py` wires adapters, per-source health/rate-limiting, and Telegram callback handling together.

**Tech Stack:** Python 3.11+, `sqlite3` (stdlib), `requests`, `feedparser` (RSS), `beautifulsoup4` (HTML scraping), `playwright` (LinkedIn auth session), `rapidfuzz` (fuzzy dedup), `pytest` + `responses` (HTTP mocking) for tests.

**Spec:** `docs/superpowers/specs/2026-09-08-ingestion-core-design.md`

## Global Constraints

- Six adapters required, each in its own file: `we_work_remotely`, `remoteok`, `remotive`, `nodesk`, `flexjobs`, `linkedin`.
- SQLite schema (`seen_jobs`, `jobs`, `source_health`) must match the spec exactly — no added/renamed columns.
- Fuzzy dedup: `rapidfuzz.fuzz.token_sort_ratio` on `"{title} {company}"`, threshold `>= 90`, lookback window 14 days.
- Source auto-disable threshold: 5 consecutive failures per source; one Telegram health alert on the failure that crosses the threshold.
- Snooze duration: 24 hours, re-alerted on the next poll cycle after expiry.
- Employment-type filter only gates `full_time` and `part_time`; `contract` and `unknown` always pass.
- Out of scope for this plan: Claude fit scoring, CV tailoring, PDF compilation, Playwright auto-apply, dashboard/setup wizard, weekly digest, calendar sync. Do not build stubs for these.

---

## File Structure

```
src/
  db.py                          # schema + connection helper
  ingestion/
    models.py                    # JobPosting dataclass
    base.py                      # SourceAdapter ABC
    health.py                    # source_health tracking
    we_work_remotely.py
    remoteok.py
    remotive.py
    nodesk.py
    flexjobs.py
    linkedin.py
  filters/
    seen.py                      # exact per-source dedup
    blacklist.py
    employment_type.py
    keywords.py
    dedup.py                     # fuzzy cross-source dedup
  notifier/
    telegram.py                  # TelegramNotifier
    callbacks.py                 # Snooze/Skip callback handling
  orchestrator.py                # process_job, resurface_snoozed
main.py                          # polling loop, wiring
config/
  sources.example.json
  blacklist.example.json
data/
  master_profile.example.json
tests/
  conftest.py
  fixtures/
    we_work_remotely.xml
    remoteok.json
    remotive.json
    nodesk.html
    flexjobs.html
    linkedin.html
  ingestion/
    test_health.py
    test_we_work_remotely.py
    test_remoteok.py
    test_remotive.py
    test_nodesk.py
    test_flexjobs.py
    test_linkedin.py
  filters/
    test_seen.py
    test_blacklist.py
    test_employment_type.py
    test_keywords.py
    test_dedup.py
  notifier/
    test_telegram.py
    test_callbacks.py
  test_db.py
  test_orchestrator.py
  test_main.py
```

---

### Task 1: Project scaffolding — DB schema, config templates, test fixtures

**Files:**
- Create: `src/__init__.py`, `src/db.py`
- Create: `config/sources.example.json`, `config/blacklist.example.json`, `data/master_profile.example.json`
- Modify: `.gitignore`
- Modify: `requirements.txt`
- Test: `tests/conftest.py`, `tests/test_db.py`

**Interfaces:**
- Produces: `get_connection(db_path: str) -> sqlite3.Connection`, `init_db(conn: sqlite3.Connection) -> None`. All later tasks use these to get a working DB.
- Produces: `tests/conftest.py` fixture `db_conn` — an initialized in-memory `sqlite3.Connection`, used by every later test that touches the DB.

- [ ] **Step 1: Write the failing test for the DB schema**

```python
# tests/test_db.py
import sqlite3
from src.db import get_connection, init_db

def test_init_db_creates_expected_tables():
    conn = get_connection(":memory:")
    init_db(conn)
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert {"seen_jobs", "jobs", "source_health"} <= tables

def test_jobs_table_enforces_unique_source_job_id():
    conn = get_connection(":memory:")
    init_db(conn)
    conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, url,
                              location, employment_type, fetched_at)
           VALUES ('wwr', '1', 't', 'c', 'u', 'l', 'unknown', '2026-01-01')"""
    )
    conn.commit()
    import pytest
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO jobs (source, source_job_id, title, company, url,
                                  location, employment_type, fetched_at)
               VALUES ('wwr', '1', 't2', 'c2', 'u2', 'l2', 'unknown', '2026-01-02')"""
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_db.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.db'`

- [ ] **Step 3: Implement `src/db.py`**

```python
# src/db.py
import sqlite3
from pathlib import Path

DB_PATH = Path("data/app_database.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_jobs (
    source TEXT NOT NULL,
    source_job_id TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    PRIMARY KEY (source, source_job_id)
);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_job_id TEXT NOT NULL,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    description TEXT,
    url TEXT NOT NULL,
    location TEXT,
    employment_type TEXT NOT NULL,
    salary_raw TEXT,
    posted_at TEXT,
    fetched_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    UNIQUE (source, source_job_id)
);

CREATE TABLE IF NOT EXISTS source_health (
    source TEXT PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_success_at TEXT,
    last_failure_at TEXT,
    disabled INTEGER NOT NULL DEFAULT 0,
    disabled_at TEXT
);
"""


def get_connection(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
```

Also create `src/__init__.py` (empty).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_db.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Add the shared `db_conn` pytest fixture**

```python
# tests/conftest.py
import pytest
from src.db import get_connection, init_db


@pytest.fixture
def db_conn():
    conn = get_connection(":memory:")
    init_db(conn)
    yield conn
    conn.close()
```

- [ ] **Step 6: Create config/data templates**

```json
// config/sources.example.json
{
  "we_work_remotely": {"enabled": true, "poll_interval_minutes": 30, "delay_range_seconds": [1, 4]},
  "remoteok":         {"enabled": true, "poll_interval_minutes": 30, "delay_range_seconds": [1, 4]},
  "remotive":         {"enabled": true, "poll_interval_minutes": 30, "delay_range_seconds": [1, 4]},
  "nodesk":           {"enabled": true, "poll_interval_minutes": 60, "delay_range_seconds": [2, 6]},
  "flexjobs":         {"enabled": true, "poll_interval_minutes": 60, "delay_range_seconds": [2, 6]},
  "linkedin":         {"enabled": true, "poll_interval_minutes": 120, "delay_range_seconds": [5, 15]}
}
```

```json
// config/blacklist.example.json
{
  "companies": [],
  "keywords": ["unpaid", "commission-only"]
}
```

```json
// data/master_profile.example.json
{
  "name": "Your Name",
  "tech_stack_keywords": ["python", "django", "react"],
  "employment_preferences": {
    "accepting_full_time": true,
    "accepting_part_time": false
  }
}
```

- [ ] **Step 7: Update `.gitignore` to exclude the real (non-example) config files**

Add these lines to `.gitignore` (keep existing lines):
```
config/sources.json
config/blacklist.json
```

- [ ] **Step 8: Update `requirements.txt`**

```
requests>=2.31
feedparser>=6.0
beautifulsoup4>=4.12
playwright>=1.40
rapidfuzz>=3.6
pytest>=8.0
responses>=0.25
python-dotenv>=1.0
```

- [ ] **Step 9: Commit**

```bash
git add src/__init__.py src/db.py tests/conftest.py tests/test_db.py \
        config/sources.example.json config/blacklist.example.json \
        data/master_profile.example.json .gitignore requirements.txt
git commit -m "feat: add DB schema, config templates, and test scaffolding"
```

---

### Task 2: JobPosting model & SourceAdapter interface

**Files:**
- Create: `src/ingestion/__init__.py`, `src/ingestion/models.py`, `src/ingestion/base.py`
- Test: `tests/ingestion/test_base.py`

**Interfaces:**
- Produces: `JobPosting` dataclass with fields `source, source_job_id, title, company, description, url, location, employment_type, salary_raw, posted_at`. Every adapter (Tasks 4-9) and the orchestrator (Task 17) construct/consume this.
- Produces: `SourceAdapter` ABC with abstract `fetch(self) -> list[JobPosting]` and class attribute `name: str`. Every adapter subclasses this.

- [ ] **Step 1: Write the failing test**

```python
# tests/ingestion/test_base.py
import pytest
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting


def test_job_posting_holds_expected_fields():
    job = JobPosting(
        source="test", source_job_id="1", title="Engineer", company="Acme",
        description="desc", url="http://x", location="Remote",
        employment_type="full_time", salary_raw=None, posted_at=None,
    )
    assert job.title == "Engineer"
    assert job.employment_type == "full_time"


def test_source_adapter_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        SourceAdapter()


def test_source_adapter_subclass_must_implement_fetch():
    class Incomplete(SourceAdapter):
        name = "incomplete"

    with pytest.raises(TypeError):
        Incomplete()

    class Complete(SourceAdapter):
        name = "complete"

        def fetch(self):
            return []

    assert Complete().fetch() == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/ingestion/test_base.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion'`

- [ ] **Step 3: Implement the model and interface**

```python
# src/ingestion/models.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class JobPosting:
    source: str
    source_job_id: str
    title: str
    company: str
    description: str
    url: str
    location: str
    employment_type: str  # 'full_time' | 'part_time' | 'contract' | 'unknown'
    salary_raw: Optional[str] = None
    posted_at: Optional[datetime] = None
```

```python
# src/ingestion/base.py
from abc import ABC, abstractmethod
from src.ingestion.models import JobPosting


class SourceAdapter(ABC):
    name: str

    @abstractmethod
    def fetch(self) -> list[JobPosting]:
        ...
```

Create empty `src/ingestion/__init__.py` and `tests/ingestion/__init__.py`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/ingestion/test_base.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/__init__.py src/ingestion/models.py src/ingestion/base.py \
        tests/ingestion/__init__.py tests/ingestion/test_base.py
git commit -m "feat: add JobPosting model and SourceAdapter interface"
```

---

### Task 3: Source health tracking

**Files:**
- Create: `src/ingestion/health.py`
- Test: `tests/ingestion/test_health.py`

**Interfaces:**
- Consumes: `db_conn` fixture from `tests/conftest.py`.
- Produces: `record_success(conn, source: str) -> None`, `record_failure(conn, source: str) -> bool` (returns True iff this call just disabled the source), `is_disabled(conn, source: str) -> bool`, `should_poll(conn, source: str, poll_interval_minutes: int) -> bool`. `main.py` (Task 18) and the orchestrator's failure handling depend on these exact signatures.

- [ ] **Step 1: Write the failing tests**

```python
# tests/ingestion/test_health.py
from src.ingestion.health import record_success, record_failure, is_disabled, should_poll


def test_should_poll_true_for_never_polled_source(db_conn):
    assert should_poll(db_conn, "wwr", 30) is True


def test_record_success_resets_failure_count(db_conn):
    for _ in range(3):
        record_failure(db_conn, "wwr")
    record_success(db_conn, "wwr")
    row = db_conn.execute(
        "SELECT consecutive_failures FROM source_health WHERE source = ?", ("wwr",)
    ).fetchone()
    assert row["consecutive_failures"] == 0


def test_record_failure_disables_after_five_consecutive():
    import sqlite3
    from src.db import get_connection, init_db

    conn = get_connection(":memory:")
    init_db(conn)
    disabled_flags = [record_failure(conn, "wwr") for _ in range(5)]
    assert disabled_flags == [False, False, False, False, True]
    assert is_disabled(conn, "wwr") is True


def test_should_poll_false_within_interval(db_conn):
    record_success(db_conn, "wwr")
    assert should_poll(db_conn, "wwr", 30) is False


def test_should_poll_true_after_interval_elapsed(db_conn):
    from datetime import datetime, timedelta
    past = (datetime.utcnow() - timedelta(minutes=31)).isoformat()
    db_conn.execute(
        """INSERT INTO source_health (source, consecutive_failures, last_success_at, disabled)
           VALUES ('wwr', 0, ?, 0)""",
        (past,),
    )
    db_conn.commit()
    assert should_poll(db_conn, "wwr", 30) is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/ingestion/test_health.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.health'`

- [ ] **Step 3: Implement `src/ingestion/health.py`**

```python
# src/ingestion/health.py
import sqlite3
from datetime import datetime, timedelta

FAILURE_THRESHOLD = 5


def record_success(conn: sqlite3.Connection, source: str) -> None:
    now = datetime.utcnow().isoformat()
    conn.execute(
        """INSERT INTO source_health (source, consecutive_failures, last_success_at, disabled)
           VALUES (?, 0, ?, 0)
           ON CONFLICT(source) DO UPDATE SET
             consecutive_failures = 0,
             last_success_at = excluded.last_success_at""",
        (source, now),
    )
    conn.commit()


def record_failure(conn: sqlite3.Connection, source: str) -> bool:
    now = datetime.utcnow().isoformat()
    row = conn.execute(
        "SELECT consecutive_failures FROM source_health WHERE source = ?", (source,)
    ).fetchone()
    failures = (row["consecutive_failures"] if row else 0) + 1
    disabled = 1 if failures >= FAILURE_THRESHOLD else 0
    conn.execute(
        """INSERT INTO source_health (source, consecutive_failures, last_failure_at, disabled, disabled_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(source) DO UPDATE SET
             consecutive_failures = excluded.consecutive_failures,
             last_failure_at = excluded.last_failure_at,
             disabled = excluded.disabled,
             disabled_at = COALESCE(source_health.disabled_at, excluded.disabled_at)""",
        (source, failures, now, disabled, now if disabled else None),
    )
    conn.commit()
    return bool(disabled)


def is_disabled(conn: sqlite3.Connection, source: str) -> bool:
    row = conn.execute(
        "SELECT disabled FROM source_health WHERE source = ?", (source,)
    ).fetchone()
    return bool(row["disabled"]) if row else False


def should_poll(conn: sqlite3.Connection, source: str, poll_interval_minutes: int) -> bool:
    row = conn.execute(
        "SELECT last_success_at, last_failure_at FROM source_health WHERE source = ?",
        (source,),
    ).fetchone()
    if row is None:
        return True
    timestamps = [t for t in (row["last_success_at"], row["last_failure_at"]) if t]
    if not timestamps:
        return True
    last = max(timestamps)
    elapsed = datetime.utcnow() - datetime.fromisoformat(last)
    return elapsed >= timedelta(minutes=poll_interval_minutes)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/ingestion/test_health.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/health.py tests/ingestion/test_health.py
git commit -m "feat: add source health tracking with auto-disable"
```

---

### Task 4: We Work Remotely adapter

**Files:**
- Create: `src/ingestion/we_work_remotely.py`
- Create: `tests/fixtures/we_work_remotely.xml`
- Test: `tests/ingestion/test_we_work_remotely.py`

**Interfaces:**
- Consumes: `JobPosting`, `SourceAdapter` from Task 2.
- Produces: `WeWorkRemotelyAdapter(feed_url: str = FEED_URL)` with `.fetch() -> list[JobPosting]`, `.name == "we_work_remotely"`.

- [ ] **Step 1: Create the fixture RSS feed**

```xml
<!-- tests/fixtures/we_work_remotely.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
  <title>We Work Remotely</title>
  <item>
    <title>Acme Corp: Senior Backend Engineer</title>
    <link>https://weworkremotely.com/jobs/1001</link>
    <guid>https://weworkremotely.com/jobs/1001</guid>
    <description>Build our Python backend.</description>
    <pubDate>Mon, 01 Sep 2026 12:00:00 +0000</pubDate>
  </item>
  <item>
    <title>Globex: Frontend Developer</title>
    <link>https://weworkremotely.com/jobs/1002</link>
    <guid>https://weworkremotely.com/jobs/1002</guid>
    <description>React and TypeScript role.</description>
    <pubDate>Tue, 02 Sep 2026 09:30:00 +0000</pubDate>
  </item>
</channel>
</rss>
```

- [ ] **Step 2: Write the failing test**

```python
# tests/ingestion/test_we_work_remotely.py
from pathlib import Path
from src.ingestion.we_work_remotely import WeWorkRemotelyAdapter

FIXTURE = str(Path(__file__).parent.parent / "fixtures" / "we_work_remotely.xml")


def test_fetch_parses_all_entries():
    adapter = WeWorkRemotelyAdapter(feed_url=FIXTURE)
    postings = adapter.fetch()
    assert len(postings) == 2


def test_fetch_splits_company_and_title():
    adapter = WeWorkRemotelyAdapter(feed_url=FIXTURE)
    postings = adapter.fetch()
    first = postings[0]
    assert first.company == "Acme Corp"
    assert first.title == "Senior Backend Engineer"
    assert first.source == "we_work_remotely"
    assert first.source_job_id == "https://weworkremotely.com/jobs/1001"
    assert first.employment_type == "unknown"
    assert first.location == "Remote"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/ingestion/test_we_work_remotely.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.we_work_remotely'`

- [ ] **Step 4: Implement the adapter**

```python
# src/ingestion/we_work_remotely.py
from datetime import datetime
import feedparser
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

FEED_URL = "https://weworkremotely.com/categories/remote-programming-jobs.rss"


class WeWorkRemotelyAdapter(SourceAdapter):
    name = "we_work_remotely"

    def __init__(self, feed_url: str = FEED_URL):
        self.feed_url = feed_url

    def fetch(self) -> list[JobPosting]:
        parsed = feedparser.parse(self.feed_url)
        postings = []
        for entry in parsed.entries:
            company, sep, title = entry.title.partition(": ")
            if not sep:
                company, title = "Unknown", entry.title
            posted_at = None
            if entry.get("published_parsed"):
                posted_at = datetime(*entry.published_parsed[:6])
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=entry.get("id", entry.link),
                    title=title,
                    company=company,
                    description=entry.get("summary", ""),
                    url=entry.link,
                    location="Remote",
                    employment_type="unknown",
                    salary_raw=None,
                    posted_at=posted_at,
                )
            )
        return postings
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/ingestion/test_we_work_remotely.py -v`
Expected: PASS (2 tests)

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/we_work_remotely.py tests/fixtures/we_work_remotely.xml \
        tests/ingestion/test_we_work_remotely.py
git commit -m "feat: add We Work Remotely ingestion adapter"
```

---

### Task 5: RemoteOK adapter

**Files:**
- Create: `src/ingestion/remoteok.py`
- Create: `tests/fixtures/remoteok.json`
- Test: `tests/ingestion/test_remoteok.py`

**Interfaces:**
- Produces: `RemoteOKAdapter(api_url: str = API_URL)` with `.fetch() -> list[JobPosting]`, `.name == "remoteok"`.

- [ ] **Step 1: Create the fixture JSON (RemoteOK's real API prepends a legal-notice object)**

```json
[
  {"legal": "Please crawl responsibly"},
  {
    "id": "2001",
    "position": "Backend Engineer",
    "company": "Acme Corp",
    "url": "https://remoteok.com/remote-jobs/2001",
    "location": "Worldwide",
    "description": "Python backend role.",
    "salary": "$80k - $110k",
    "date": "2026-09-01T00:00:00"
  }
]
```

- [ ] **Step 2: Write the failing test**

```python
# tests/ingestion/test_remoteok.py
import json
from pathlib import Path
import responses
from src.ingestion.remoteok import RemoteOKAdapter

FIXTURE = json.loads((Path(__file__).parent.parent / "fixtures" / "remoteok.json").read_text())
API_URL = "https://remoteok.com/api"


@responses.activate
def test_fetch_skips_legal_notice_and_parses_jobs():
    responses.add(responses.GET, API_URL, json=FIXTURE, status=200)
    adapter = RemoteOKAdapter(api_url=API_URL)
    postings = adapter.fetch()
    assert len(postings) == 1
    job = postings[0]
    assert job.source == "remoteok"
    assert job.source_job_id == "2001"
    assert job.title == "Backend Engineer"
    assert job.company == "Acme Corp"
    assert job.employment_type == "unknown"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/ingestion/test_remoteok.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.remoteok'`

- [ ] **Step 4: Implement the adapter**

```python
# src/ingestion/remoteok.py
from datetime import datetime
import requests
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

API_URL = "https://remoteok.com/api"


class RemoteOKAdapter(SourceAdapter):
    name = "remoteok"

    def __init__(self, api_url: str = API_URL):
        self.api_url = api_url

    def fetch(self) -> list[JobPosting]:
        response = requests.get(self.api_url, headers={"User-Agent": "job-agent/1.0"}, timeout=10)
        response.raise_for_status()
        postings = []
        for item in response.json():
            if "id" not in item:
                continue  # legal-notice entry, not a job
            posted_at = None
            if item.get("date"):
                posted_at = datetime.fromisoformat(item["date"])
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=str(item["id"]),
                    title=item.get("position", ""),
                    company=item.get("company", "Unknown"),
                    description=item.get("description", ""),
                    url=item.get("url", ""),
                    location=item.get("location") or "Remote",
                    employment_type="unknown",
                    salary_raw=item.get("salary") or None,
                    posted_at=posted_at,
                )
            )
        return postings
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/ingestion/test_remoteok.py -v`
Expected: PASS (1 test)

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/remoteok.py tests/fixtures/remoteok.json tests/ingestion/test_remoteok.py
git commit -m "feat: add RemoteOK ingestion adapter"
```

---

### Task 6: Remotive adapter

**Files:**
- Create: `src/ingestion/remotive.py`
- Create: `tests/fixtures/remotive.json`
- Test: `tests/ingestion/test_remotive.py`

**Interfaces:**
- Produces: `RemotiveAdapter(api_url: str = API_URL)` with `.fetch() -> list[JobPosting]`, `.name == "remotive"`. This is the only adapter that reliably reports `employment_type` (Remotive's `job_type` field), so its correct mapping to `full_time`/`part_time`/`contract` matters for the employment-type filter (Task 11).

- [ ] **Step 1: Create the fixture JSON**

```json
{
  "jobs": [
    {
      "id": 3001,
      "title": "Full Stack Engineer",
      "company_name": "Acme Corp",
      "url": "https://remotive.com/remote-jobs/3001",
      "candidate_required_location": "Worldwide",
      "job_type": "full_time",
      "description": "Django and React.",
      "salary": "",
      "publication_date": "2026-09-01T00:00:00Z"
    },
    {
      "id": 3002,
      "title": "Part-Time Designer",
      "company_name": "Globex",
      "url": "https://remotive.com/remote-jobs/3002",
      "candidate_required_location": "US Only",
      "job_type": "part_time",
      "description": "Figma design work.",
      "salary": "",
      "publication_date": "2026-09-02T00:00:00Z"
    }
  ]
}
```

- [ ] **Step 2: Write the failing test**

```python
# tests/ingestion/test_remotive.py
import json
from pathlib import Path
import responses
from src.ingestion.remotive import RemotiveAdapter

FIXTURE = json.loads((Path(__file__).parent.parent / "fixtures" / "remotive.json").read_text())
API_URL = "https://remotive.com/api/remote-jobs"


@responses.activate
def test_fetch_maps_job_type_to_employment_type():
    responses.add(responses.GET, API_URL, json=FIXTURE, status=200)
    adapter = RemotiveAdapter(api_url=API_URL)
    postings = adapter.fetch()
    assert len(postings) == 2
    assert postings[0].employment_type == "full_time"
    assert postings[1].employment_type == "part_time"
    assert postings[0].company == "Acme Corp"
    assert postings[0].source_job_id == "3001"


@responses.activate
def test_fetch_defaults_unknown_job_type_to_unknown():
    fixture = {"jobs": [dict(FIXTURE["jobs"][0], job_type="internship")]}
    responses.add(responses.GET, API_URL, json=fixture, status=200)
    adapter = RemotiveAdapter(api_url=API_URL)
    postings = adapter.fetch()
    assert postings[0].employment_type == "unknown"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/ingestion/test_remotive.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.remotive'`

- [ ] **Step 4: Implement the adapter**

```python
# src/ingestion/remotive.py
from datetime import datetime
import requests
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

API_URL = "https://remotive.com/api/remote-jobs"

EMPLOYMENT_TYPE_MAP = {
    "full_time": "full_time",
    "part_time": "part_time",
    "contract": "contract",
    "freelance": "contract",
}


class RemotiveAdapter(SourceAdapter):
    name = "remotive"

    def __init__(self, api_url: str = API_URL):
        self.api_url = api_url

    def fetch(self) -> list[JobPosting]:
        response = requests.get(self.api_url, timeout=10)
        response.raise_for_status()
        postings = []
        for item in response.json().get("jobs", []):
            posted_at = None
            if item.get("publication_date"):
                posted_at = datetime.fromisoformat(item["publication_date"].replace("Z", "+00:00"))
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=str(item["id"]),
                    title=item.get("title", ""),
                    company=item.get("company_name", "Unknown"),
                    description=item.get("description", ""),
                    url=item.get("url", ""),
                    location=item.get("candidate_required_location") or "Remote",
                    employment_type=EMPLOYMENT_TYPE_MAP.get(item.get("job_type"), "unknown"),
                    salary_raw=item.get("salary") or None,
                    posted_at=posted_at,
                )
            )
        return postings
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/ingestion/test_remotive.py -v`
Expected: PASS (2 tests)

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/remotive.py tests/fixtures/remotive.json tests/ingestion/test_remotive.py
git commit -m "feat: add Remotive ingestion adapter with employment-type mapping"
```

---

### Task 7: NoDesk adapter

**Files:**
- Create: `src/ingestion/nodesk.py`
- Create: `tests/fixtures/nodesk.html`
- Test: `tests/ingestion/test_nodesk.py`

**Interfaces:**
- Produces: `NoDeskAdapter(listing_url: str = LISTING_URL)` with `.fetch() -> list[JobPosting]` and a separately-callable `.parse_html(html: str) -> list[JobPosting]`, `.name == "nodesk"`.

**Note:** NoDesk has no public API, so this is an HTML scrape. The selectors below (`a.job-card`, `.job-card__title`, etc.) are a best-effort guess at the site's structure at design time, encoded in the fixture used to test them. If the live site's markup differs, `fetch()` will simply return an empty list — Source Health (Task 3) will auto-disable the source after 5 empty/erroring cycles and alert you, at which point update the selectors in this file to match the real markup.

- [ ] **Step 1: Create the fixture HTML**

```html
<!-- tests/fixtures/nodesk.html -->
<html><body>
<div class="job-listings">
  <a class="job-card" href="/remote-jobs/acme-backend-engineer">
    <span class="job-card__title">Backend Engineer</span>
    <span class="job-card__company">Acme Corp</span>
    <span class="job-card__location">Remote (US)</span>
  </a>
  <a class="job-card" href="https://nodesk.co/remote-jobs/globex-designer">
    <span class="job-card__title">Product Designer</span>
    <span class="job-card__company">Globex</span>
    <span class="job-card__location">Remote (Worldwide)</span>
  </a>
</div>
</body></html>
```

- [ ] **Step 2: Write the failing test**

```python
# tests/ingestion/test_nodesk.py
from pathlib import Path
from src.ingestion.nodesk import NoDeskAdapter

FIXTURE_HTML = (Path(__file__).parent.parent / "fixtures" / "nodesk.html").read_text()


def test_parse_html_extracts_jobs_and_normalizes_relative_url():
    adapter = NoDeskAdapter()
    postings = adapter.parse_html(FIXTURE_HTML)
    assert len(postings) == 2
    first = postings[0]
    assert first.title == "Backend Engineer"
    assert first.company == "Acme Corp"
    assert first.source == "nodesk"
    assert first.source_job_id == "acme-backend-engineer"
    assert first.url == "https://nodesk.co/remote-jobs/acme-backend-engineer"
    assert first.employment_type == "unknown"


def test_parse_html_keeps_absolute_url_unchanged():
    adapter = NoDeskAdapter()
    postings = adapter.parse_html(FIXTURE_HTML)
    assert postings[1].url == "https://nodesk.co/remote-jobs/globex-designer"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/ingestion/test_nodesk.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.nodesk'`

- [ ] **Step 4: Implement the adapter**

```python
# src/ingestion/nodesk.py
import requests
from bs4 import BeautifulSoup
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

LISTING_URL = "https://nodesk.co/remote-jobs/"


class NoDeskAdapter(SourceAdapter):
    name = "nodesk"

    def __init__(self, listing_url: str = LISTING_URL):
        self.listing_url = listing_url

    def fetch(self) -> list[JobPosting]:
        response = requests.get(self.listing_url, headers={"User-Agent": "job-agent/1.0"}, timeout=10)
        response.raise_for_status()
        return self.parse_html(response.text)

    def parse_html(self, html: str) -> list[JobPosting]:
        soup = BeautifulSoup(html, "html.parser")
        postings = []
        for card in soup.select("a.job-card"):
            href = card.get("href", "")
            url = href if href.startswith("http") else f"https://nodesk.co{href}"
            source_job_id = url.rstrip("/").rsplit("/", 1)[-1]
            title = card.select_one(".job-card__title")
            company = card.select_one(".job-card__company")
            location = card.select_one(".job-card__location")
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=source_job_id,
                    title=title.get_text(strip=True) if title else "",
                    company=company.get_text(strip=True) if company else "Unknown",
                    description="",
                    url=url,
                    location=location.get_text(strip=True) if location else "Remote",
                    employment_type="unknown",
                    salary_raw=None,
                    posted_at=None,
                )
            )
        return postings
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/ingestion/test_nodesk.py -v`
Expected: PASS (2 tests)

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/nodesk.py tests/fixtures/nodesk.html tests/ingestion/test_nodesk.py
git commit -m "feat: add NoDesk ingestion adapter"
```

---

### Task 8: FlexJobs adapter

**Files:**
- Create: `src/ingestion/flexjobs.py`
- Create: `tests/fixtures/flexjobs.html`
- Test: `tests/ingestion/test_flexjobs.py`

**Interfaces:**
- Produces: `FlexJobsAdapter(listing_url: str = LISTING_URL)` with `.fetch() -> list[JobPosting]` and `.parse_html(html: str) -> list[JobPosting]`, `.name == "flexjobs"`.

**Note:** FlexJobs gates full descriptions behind a paid account; this adapter parses only the public teaser listing (title, company, location, a truncated snippet). Like NoDesk, the CSS selectors are a best-effort guess — if the real site differs, Source Health will surface it via auto-disable rather than a silent failure.

- [ ] **Step 1: Create the fixture HTML**

```html
<!-- tests/fixtures/flexjobs.html -->
<html><body>
<ul class="job-list">
  <li class="job-teaser" data-job-id="4001">
    <a class="job-teaser__link" href="/remote-jobs/backend-engineer-acme">Backend Engineer</a>
    <span class="job-teaser__company">Acme Corp</span>
    <span class="job-teaser__location">Remote</span>
    <span class="job-teaser__snippet">Part-time Python role...</span>
  </li>
</ul>
</body></html>
```

- [ ] **Step 2: Write the failing test**

```python
# tests/ingestion/test_flexjobs.py
from pathlib import Path
from src.ingestion.flexjobs import FlexJobsAdapter

FIXTURE_HTML = (Path(__file__).parent.parent / "fixtures" / "flexjobs.html").read_text()


def test_parse_html_extracts_teaser_fields():
    adapter = FlexJobsAdapter()
    postings = adapter.parse_html(FIXTURE_HTML)
    assert len(postings) == 1
    job = postings[0]
    assert job.source == "flexjobs"
    assert job.source_job_id == "4001"
    assert job.title == "Backend Engineer"
    assert job.company == "Acme Corp"
    assert job.description == "Part-time Python role..."
    assert job.url == "https://www.flexjobs.com/remote-jobs/backend-engineer-acme"
    assert job.employment_type == "unknown"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/ingestion/test_flexjobs.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.flexjobs'`

- [ ] **Step 4: Implement the adapter**

```python
# src/ingestion/flexjobs.py
import requests
from bs4 import BeautifulSoup
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

LISTING_URL = "https://www.flexjobs.com/remote-jobs"
BASE_URL = "https://www.flexjobs.com"


class FlexJobsAdapter(SourceAdapter):
    name = "flexjobs"

    def __init__(self, listing_url: str = LISTING_URL):
        self.listing_url = listing_url

    def fetch(self) -> list[JobPosting]:
        response = requests.get(self.listing_url, headers={"User-Agent": "job-agent/1.0"}, timeout=10)
        response.raise_for_status()
        return self.parse_html(response.text)

    def parse_html(self, html: str) -> list[JobPosting]:
        soup = BeautifulSoup(html, "html.parser")
        postings = []
        for teaser in soup.select("li.job-teaser"):
            link = teaser.select_one(".job-teaser__link")
            company = teaser.select_one(".job-teaser__company")
            location = teaser.select_one(".job-teaser__location")
            snippet = teaser.select_one(".job-teaser__snippet")
            href = link.get("href", "") if link else ""
            url = href if href.startswith("http") else f"{BASE_URL}{href}"
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=teaser.get("data-job-id", url),
                    title=link.get_text(strip=True) if link else "",
                    company=company.get_text(strip=True) if company else "Unknown",
                    description=snippet.get_text(strip=True) if snippet else "",
                    url=url,
                    location=location.get_text(strip=True) if location else "Remote",
                    employment_type="unknown",
                    salary_raw=None,
                    posted_at=None,
                )
            )
        return postings
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/ingestion/test_flexjobs.py -v`
Expected: PASS (1 test)

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/flexjobs.py tests/fixtures/flexjobs.html tests/ingestion/test_flexjobs.py
git commit -m "feat: add FlexJobs ingestion adapter"
```

---

### Task 9: LinkedIn adapter

**Files:**
- Create: `src/ingestion/linkedin.py`
- Create: `tests/fixtures/linkedin.html`
- Test: `tests/ingestion/test_linkedin.py`

**Interfaces:**
- Produces: `LinkedInAdapter(storage_state_path: str = STORAGE_STATE_PATH)` with `.fetch() -> list[JobPosting]` (Playwright-driven, not covered by automated tests) and `.parse_html(html: str) -> list[JobPosting]` (pure parsing, fully tested), `.name == "linkedin"`.

**Note:** LinkedIn requires an authenticated session and carries the highest ban/breakage risk of all six sources — exactly what Source Health exists to contain. Automating the *login* itself is out of scope for this plan: before running this adapter for real, generate `data/linkedin_storage_state.json` once via `playwright codegen --save-storage=data/linkedin_storage_state.json https://www.linkedin.com/login`, log in manually in the opened browser, then close it. `fetch()` reuses that saved session and is verified manually (see Step 6); only `parse_html()` is covered by the automated test suite.

- [ ] **Step 1: Create the fixture HTML**

```html
<!-- tests/fixtures/linkedin.html -->
<html><body>
<ul class="jobs-search__results-list">
  <li>
    <div class="base-card" data-entity-urn="urn:li:jobPosting:5001">
      <h3 class="base-search-card__title">Backend Engineer</h3>
      <h4 class="base-search-card__subtitle">Acme Corp</h4>
      <span class="job-search-card__location">Remote</span>
      <a class="base-card__full-link" href="https://www.linkedin.com/jobs/view/5001">link</a>
    </div>
  </li>
</ul>
</body></html>
```

- [ ] **Step 2: Write the failing test**

```python
# tests/ingestion/test_linkedin.py
from pathlib import Path
from src.ingestion.linkedin import LinkedInAdapter

FIXTURE_HTML = (Path(__file__).parent.parent / "fixtures" / "linkedin.html").read_text()


def test_parse_html_extracts_job_id_from_entity_urn():
    adapter = LinkedInAdapter()
    postings = adapter.parse_html(FIXTURE_HTML)
    assert len(postings) == 1
    job = postings[0]
    assert job.source == "linkedin"
    assert job.source_job_id == "5001"
    assert job.title == "Backend Engineer"
    assert job.company == "Acme Corp"
    assert job.location == "Remote"
    assert job.url == "https://www.linkedin.com/jobs/view/5001"
    assert job.employment_type == "unknown"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/ingestion/test_linkedin.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.ingestion.linkedin'`

- [ ] **Step 4: Implement the adapter**

```python
# src/ingestion/linkedin.py
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from src.ingestion.base import SourceAdapter
from src.ingestion.models import JobPosting

SEARCH_URL = "https://www.linkedin.com/jobs/search/?keywords=remote"
STORAGE_STATE_PATH = "data/linkedin_storage_state.json"


class LinkedInAdapter(SourceAdapter):
    name = "linkedin"

    def __init__(self, storage_state_path: str = STORAGE_STATE_PATH, search_url: str = SEARCH_URL):
        self.storage_state_path = storage_state_path
        self.search_url = search_url

    def fetch(self) -> list[JobPosting]:  # pragma: no cover — requires a live authenticated session
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(storage_state=self.storage_state_path)
            page = context.new_page()
            page.goto(self.search_url)
            page.wait_for_selector(".jobs-search__results-list")
            html = page.content()
            browser.close()
        return self.parse_html(html)

    def parse_html(self, html: str) -> list[JobPosting]:
        soup = BeautifulSoup(html, "html.parser")
        postings = []
        for card in soup.select(".base-card"):
            urn = card.get("data-entity-urn", "")
            job_id = urn.rsplit(":", 1)[-1] if urn else ""
            title = card.select_one(".base-search-card__title")
            company = card.select_one(".base-search-card__subtitle")
            location = card.select_one(".job-search-card__location")
            link = card.select_one(".base-card__full-link")
            postings.append(
                JobPosting(
                    source=self.name,
                    source_job_id=job_id,
                    title=title.get_text(strip=True) if title else "",
                    company=company.get_text(strip=True) if company else "Unknown",
                    description="",
                    url=link.get("href", "") if link else "",
                    location=location.get_text(strip=True) if location else "Remote",
                    employment_type="unknown",
                    salary_raw=None,
                    posted_at=None,
                )
            )
        return postings
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/ingestion/test_linkedin.py -v`
Expected: PASS (1 test)

- [ ] **Step 6: Manual verification note (not automated)**

After generating `data/linkedin_storage_state.json` as described above, run `python -c "from src.ingestion.linkedin import LinkedInAdapter; print(LinkedInAdapter().fetch())"` once manually and confirm it returns real postings without raising. This step is documentation only — do not add it to the pytest suite, since it requires live network access and a real logged-in session.

- [ ] **Step 7: Commit**

```bash
git add src/ingestion/linkedin.py tests/fixtures/linkedin.html tests/ingestion/test_linkedin.py
git commit -m "feat: add LinkedIn ingestion adapter"
```

---

### Task 10: Blacklist filter

**Files:**
- Create: `src/filters/__init__.py`, `src/filters/blacklist.py`
- Test: `tests/filters/test_blacklist.py`

**Interfaces:**
- Consumes: `JobPosting` from Task 2.
- Produces: `is_blacklisted(job: JobPosting, blacklist: dict) -> bool`. Used by the orchestrator (Task 17).

- [ ] **Step 1: Write the failing test**

```python
# tests/filters/test_blacklist.py
from src.ingestion.models import JobPosting
from src.filters.blacklist import is_blacklisted

BLACKLIST = {"companies": ["Shady Recruiting Inc"], "keywords": ["unpaid", "commission-only"]}


def make_job(**overrides):
    defaults = dict(
        source="test", source_job_id="1", title="Engineer", company="Acme",
        description="Great role", url="http://x", location="Remote",
        employment_type="full_time", salary_raw=None, posted_at=None,
    )
    defaults.update(overrides)
    return JobPosting(**defaults)


def test_blocks_blacklisted_company_case_insensitive():
    job = make_job(company="shady recruiting inc")
    assert is_blacklisted(job, BLACKLIST) is True


def test_blocks_blacklisted_keyword_in_description():
    job = make_job(description="This is an unpaid internship")
    assert is_blacklisted(job, BLACKLIST) is True


def test_passes_job_matching_neither():
    job = make_job()
    assert is_blacklisted(job, BLACKLIST) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/filters/test_blacklist.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.filters'`

- [ ] **Step 3: Implement the filter**

```python
# src/filters/blacklist.py
from src.ingestion.models import JobPosting


def is_blacklisted(job: JobPosting, blacklist: dict) -> bool:
    blacklisted_companies = {c.strip().lower() for c in blacklist.get("companies", [])}
    if job.company.strip().lower() in blacklisted_companies:
        return True
    text = f"{job.title} {job.description}".lower()
    return any(keyword.strip().lower() in text for keyword in blacklist.get("keywords", []))
```

Create empty `src/filters/__init__.py` and `tests/filters/__init__.py`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/filters/test_blacklist.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/filters/__init__.py src/filters/blacklist.py \
        tests/filters/__init__.py tests/filters/test_blacklist.py
git commit -m "feat: add blacklist filter"
```

---

### Task 11: Employment-type filter

**Files:**
- Create: `src/filters/employment_type.py`
- Test: `tests/filters/test_employment_type.py`

**Interfaces:**
- Produces: `passes_employment_filter(job: JobPosting, preferences: dict) -> bool`. Used by the orchestrator (Task 17). `preferences` is `master_profile.json["employment_preferences"]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/filters/test_employment_type.py
from src.ingestion.models import JobPosting
from src.filters.employment_type import passes_employment_filter


def make_job(employment_type):
    return JobPosting(
        source="test", source_job_id="1", title="Engineer", company="Acme",
        description="", url="http://x", location="Remote",
        employment_type=employment_type, salary_raw=None, posted_at=None,
    )


def test_full_time_blocked_when_not_accepting_full_time():
    prefs = {"accepting_full_time": False, "accepting_part_time": True}
    assert passes_employment_filter(make_job("full_time"), prefs) is False


def test_part_time_allowed_when_accepting_part_time():
    prefs = {"accepting_full_time": False, "accepting_part_time": True}
    assert passes_employment_filter(make_job("part_time"), prefs) is True


def test_contract_and_unknown_always_pass():
    prefs = {"accepting_full_time": False, "accepting_part_time": False}
    assert passes_employment_filter(make_job("contract"), prefs) is True
    assert passes_employment_filter(make_job("unknown"), prefs) is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/filters/test_employment_type.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.filters.employment_type'`

- [ ] **Step 3: Implement the filter**

```python
# src/filters/employment_type.py
from src.ingestion.models import JobPosting


def passes_employment_filter(job: JobPosting, preferences: dict) -> bool:
    if job.employment_type == "full_time":
        return preferences.get("accepting_full_time", True)
    if job.employment_type == "part_time":
        return preferences.get("accepting_part_time", True)
    return True
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/filters/test_employment_type.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/filters/employment_type.py tests/filters/test_employment_type.py
git commit -m "feat: add employment-type preference filter"
```

---

### Task 12: Tech-stack keyword filter

**Files:**
- Create: `src/filters/keywords.py`
- Test: `tests/filters/test_keywords.py`

**Interfaces:**
- Produces: `matches_tech_stack(job: JobPosting, keywords: list[str]) -> bool`. Used by the orchestrator (Task 17). `keywords` is `master_profile.json["tech_stack_keywords"]`.

- [ ] **Step 1: Write the failing test**

```python
# tests/filters/test_keywords.py
from src.ingestion.models import JobPosting
from src.filters.keywords import matches_tech_stack


def make_job(title, description=""):
    return JobPosting(
        source="test", source_job_id="1", title=title, company="Acme",
        description=description, url="http://x", location="Remote",
        employment_type="unknown", salary_raw=None, posted_at=None,
    )


def test_matches_keyword_in_title():
    job = make_job("Senior Python Engineer")
    assert matches_tech_stack(job, ["python", "django"]) is True


def test_matches_keyword_in_description_case_insensitive():
    job = make_job("Engineer", description="Must know REACT well")
    assert matches_tech_stack(job, ["react"]) is True


def test_no_match_returns_false():
    job = make_job("Sales Representative")
    assert matches_tech_stack(job, ["python", "react"]) is False


def test_empty_keyword_list_passes_everything():
    job = make_job("Sales Representative")
    assert matches_tech_stack(job, []) is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/filters/test_keywords.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.filters.keywords'`

- [ ] **Step 3: Implement the filter**

```python
# src/filters/keywords.py
from src.ingestion.models import JobPosting


def matches_tech_stack(job: JobPosting, keywords: list[str]) -> bool:
    if not keywords:
        return True
    text = f"{job.title} {job.description}".lower()
    return any(keyword.strip().lower() in text for keyword in keywords)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/filters/test_keywords.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/filters/keywords.py tests/filters/test_keywords.py
git commit -m "feat: add tech-stack keyword pre-filter"
```

---

### Task 13: Fuzzy cross-source dedup

**Files:**
- Create: `src/filters/dedup.py`
- Test: `tests/filters/test_dedup.py`

**Interfaces:**
- Consumes: `db_conn` fixture; reads the `jobs` table.
- Produces: `is_duplicate(conn, job: JobPosting) -> bool`. Used by the orchestrator (Task 17), called only on jobs that already passed blacklist/employment/keyword filters.

- [ ] **Step 1: Write the failing test**

```python
# tests/filters/test_dedup.py
from datetime import datetime, timedelta
from src.ingestion.models import JobPosting
from src.filters.dedup import is_duplicate


def make_job(title, company):
    return JobPosting(
        source="test", source_job_id="1", title=title, company=company,
        description="", url="http://x", location="Remote",
        employment_type="unknown", salary_raw=None, posted_at=None,
    )


def insert_job(conn, title, company, fetched_at):
    conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, url,
                              location, employment_type, fetched_at)
           VALUES ('other_source', ?, ?, ?, 'http://y', 'Remote', 'unknown', ?)""",
        (title, title, company, fetched_at),
    )
    conn.commit()


def test_detects_reordered_title_as_duplicate(db_conn):
    insert_job(db_conn, "Senior Backend Engineer", "Acme Corp", datetime.utcnow().isoformat())
    job = make_job("Backend Engineer, Senior", "Acme Corp")
    assert is_duplicate(db_conn, job) is True


def test_does_not_flag_clearly_different_jobs(db_conn):
    insert_job(db_conn, "Senior Backend Engineer", "Acme Corp", datetime.utcnow().isoformat())
    job = make_job("Junior Frontend Developer", "Globex")
    assert is_duplicate(db_conn, job) is False


def test_ignores_jobs_outside_lookback_window(db_conn):
    old = (datetime.utcnow() - timedelta(days=20)).isoformat()
    insert_job(db_conn, "Senior Backend Engineer", "Acme Corp", old)
    job = make_job("Senior Backend Engineer", "Acme Corp")
    assert is_duplicate(db_conn, job) is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/filters/test_dedup.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.filters.dedup'`

- [ ] **Step 3: Implement the filter**

```python
# src/filters/dedup.py
import sqlite3
from datetime import datetime, timedelta
from rapidfuzz import fuzz
from src.ingestion.models import JobPosting

SIMILARITY_THRESHOLD = 90
LOOKBACK_DAYS = 14


def is_duplicate(conn: sqlite3.Connection, job: JobPosting) -> bool:
    cutoff = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    rows = conn.execute(
        "SELECT title, company FROM jobs WHERE fetched_at >= ?", (cutoff,)
    ).fetchall()
    candidate = f"{job.title} {job.company}".lower()
    for row in rows:
        existing = f"{row['title']} {row['company']}".lower()
        if fuzz.token_sort_ratio(candidate, existing) >= SIMILARITY_THRESHOLD:
            return True
    return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/filters/test_dedup.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/filters/dedup.py tests/filters/test_dedup.py
git commit -m "feat: add fuzzy cross-source dedup filter"
```

---

### Task 14: Seen-job check

**Files:**
- Create: `src/filters/seen.py`
- Test: `tests/filters/test_seen.py`

**Interfaces:**
- Produces: `has_seen(conn, source: str, source_job_id: str) -> bool`, `mark_seen(conn, source: str, source_job_id: str) -> None`. Used first in the orchestrator's pipeline (Task 17).

- [ ] **Step 1: Write the failing test**

```python
# tests/filters/test_seen.py
from src.filters.seen import has_seen, mark_seen


def test_has_seen_false_before_marking(db_conn):
    assert has_seen(db_conn, "wwr", "job-1") is False


def test_has_seen_true_after_marking(db_conn):
    mark_seen(db_conn, "wwr", "job-1")
    assert has_seen(db_conn, "wwr", "job-1") is True


def test_mark_seen_is_idempotent(db_conn):
    mark_seen(db_conn, "wwr", "job-1")
    mark_seen(db_conn, "wwr", "job-1")  # must not raise
    assert has_seen(db_conn, "wwr", "job-1") is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/filters/test_seen.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.filters.seen'`

- [ ] **Step 3: Implement**

```python
# src/filters/seen.py
import sqlite3
from datetime import datetime


def has_seen(conn: sqlite3.Connection, source: str, source_job_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM seen_jobs WHERE source = ? AND source_job_id = ?",
        (source, source_job_id),
    ).fetchone()
    return row is not None


def mark_seen(conn: sqlite3.Connection, source: str, source_job_id: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO seen_jobs (source, source_job_id, first_seen_at) VALUES (?, ?, ?)",
        (source, source_job_id, datetime.utcnow().isoformat()),
    )
    conn.commit()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/filters/test_seen.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/filters/seen.py tests/filters/test_seen.py
git commit -m "feat: add exact per-source seen-job check"
```

---

### Task 15: Telegram notifier

**Files:**
- Create: `src/notifier/__init__.py`, `src/notifier/telegram.py`
- Test: `tests/notifier/test_telegram.py`

**Interfaces:**
- Produces: `TelegramNotifier(bot_token: str, chat_id: str, api_base: str = API_BASE)` with methods `.send_job_alert(job_id: int, title: str, company: str, location: str, employment_type: str, source: str, url: str) -> dict`, `.send_health_alert(source: str, last_error: str) -> dict`, `.get_callback_updates(offset: int | None = None) -> list[dict]`. Used by the orchestrator (Task 17) and `main.py` (Task 18).

- [ ] **Step 1: Write the failing tests**

```python
# tests/notifier/test_telegram.py
import responses
from src.notifier.telegram import TelegramNotifier

API_BASE = "https://api.telegram.org"


@responses.activate
def test_send_job_alert_posts_expected_payload():
    responses.add(
        responses.POST, f"{API_BASE}/bottoken123/sendMessage",
        json={"ok": True, "result": {"message_id": 1}}, status=200,
    )
    notifier = TelegramNotifier(bot_token="token123", chat_id="chat1", api_base=API_BASE)
    notifier.send_job_alert(
        job_id=42, title="Backend Engineer", company="Acme", location="Remote",
        employment_type="full_time", source="remoteok", url="http://x/42",
    )
    request = responses.calls[0].request
    import json
    payload = json.loads(request.body)
    assert payload["chat_id"] == "chat1"
    assert "Backend Engineer" in payload["text"]
    assert "Acme" in payload["text"]
    buttons = payload["reply_markup"]["inline_keyboard"][0]
    assert buttons[0]["callback_data"] == "snooze:42"
    assert buttons[1]["callback_data"] == "skip:42"


@responses.activate
def test_send_health_alert_posts_source_and_error():
    responses.add(
        responses.POST, f"{API_BASE}/bottoken123/sendMessage",
        json={"ok": True, "result": {"message_id": 2}}, status=200,
    )
    notifier = TelegramNotifier(bot_token="token123", chat_id="chat1", api_base=API_BASE)
    notifier.send_health_alert(source="linkedin", last_error="Timeout")
    import json
    payload = json.loads(responses.calls[0].request.body)
    assert "linkedin" in payload["text"]
    assert "Timeout" in payload["text"]


@responses.activate
def test_get_callback_updates_passes_offset_and_parses_result():
    responses.add(
        responses.GET, f"{API_BASE}/bottoken123/getUpdates",
        json={"ok": True, "result": [{"update_id": 5, "callback_query": {"data": "skip:1"}}]},
        status=200,
    )
    notifier = TelegramNotifier(bot_token="token123", chat_id="chat1", api_base=API_BASE)
    updates = notifier.get_callback_updates(offset=6)
    assert updates == [{"update_id": 5, "callback_query": {"data": "skip:1"}}]
    assert responses.calls[0].request.params["offset"] == "6"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/notifier/test_telegram.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.notifier'`

- [ ] **Step 3: Implement**

```python
# src/notifier/telegram.py
import requests

API_BASE = "https://api.telegram.org"


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, api_base: str = API_BASE):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_base = api_base

    def _url(self, method: str) -> str:
        return f"{self.api_base}/bot{self.bot_token}/{method}"

    def send_job_alert(self, job_id: int, title: str, company: str, location: str,
                        employment_type: str, source: str, url: str) -> dict:
        text = f"{title} at {company}\n{location} | {employment_type} | via {source}\n{url}"
        keyboard = {
            "inline_keyboard": [[
                {"text": "Snooze 24h", "callback_data": f"snooze:{job_id}"},
                {"text": "Skip", "callback_data": f"skip:{job_id}"},
            ]]
        }
        response = requests.post(
            self._url("sendMessage"),
            json={"chat_id": self.chat_id, "text": text, "reply_markup": keyboard},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def send_health_alert(self, source: str, last_error: str) -> dict:
        text = f"Source {source} disabled after repeated failures.\nLast error: {last_error}"
        response = requests.post(
            self._url("sendMessage"),
            json={"chat_id": self.chat_id, "text": text},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def get_callback_updates(self, offset: int | None = None) -> list[dict]:
        params = {"timeout": 0}
        if offset is not None:
            params["offset"] = offset
        response = requests.get(self._url("getUpdates"), params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("result", [])
```

Create empty `src/notifier/__init__.py` and `tests/notifier/__init__.py`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/notifier/test_telegram.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/notifier/__init__.py src/notifier/telegram.py \
        tests/notifier/__init__.py tests/notifier/test_telegram.py
git commit -m "feat: add Telegram notifier with job and health alerts"
```

---

### Task 16: Snooze/Skip callback handling

**Files:**
- Create: `src/notifier/callbacks.py`
- Test: `tests/notifier/test_callbacks.py`

**Interfaces:**
- Produces: `apply_callback(conn, callback_data: str) -> str` (returns the action name). Used by `main.py` (Task 18).
- Note: on `snooze`, this repurposes the `jobs.fetched_at` column as a "snoozed-since" timestamp — there is no separate `snoozed_until` column in the spec's schema, so `resurface_snoozed` (Task 17) computes the 24-hour expiry from this reused field.

- [ ] **Step 1: Write the failing test**

```python
# tests/notifier/test_callbacks.py
import pytest
from src.notifier.callbacks import apply_callback


def insert_job(conn):
    conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, url,
                              location, employment_type, fetched_at, status)
           VALUES ('wwr', '1', 't', 'c', 'u', 'l', 'unknown', '2026-01-01', 'alerted')"""
    )
    conn.commit()
    return conn.execute("SELECT id FROM jobs").fetchone()["id"]


def test_snooze_sets_status_and_updates_fetched_at(db_conn):
    job_id = insert_job(db_conn)
    action = apply_callback(db_conn, f"snooze:{job_id}")
    assert action == "snooze"
    row = db_conn.execute("SELECT status, fetched_at FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert row["status"] == "snoozed"
    assert row["fetched_at"] != "2026-01-01"


def test_skip_sets_status(db_conn):
    job_id = insert_job(db_conn)
    action = apply_callback(db_conn, f"skip:{job_id}")
    assert action == "skip"
    row = db_conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert row["status"] == "skipped"


def test_unknown_action_raises(db_conn):
    job_id = insert_job(db_conn)
    with pytest.raises(ValueError):
        apply_callback(db_conn, f"bogus:{job_id}")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/notifier/test_callbacks.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.notifier.callbacks'`

- [ ] **Step 3: Implement**

```python
# src/notifier/callbacks.py
import sqlite3
from datetime import datetime


def apply_callback(conn: sqlite3.Connection, callback_data: str) -> str:
    action, _, job_id_str = callback_data.partition(":")
    job_id = int(job_id_str)
    if action == "snooze":
        conn.execute(
            "UPDATE jobs SET status = 'snoozed', fetched_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), job_id),
        )
    elif action == "skip":
        conn.execute("UPDATE jobs SET status = 'skipped' WHERE id = ?", (job_id,))
    else:
        raise ValueError(f"Unknown callback action: {action}")
    conn.commit()
    return action
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/notifier/test_callbacks.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/notifier/callbacks.py tests/notifier/test_callbacks.py
git commit -m "feat: add Snooze/Skip Telegram callback handling"
```

---

### Task 17: Orchestrator — pipeline wiring and snooze resurfacing

**Files:**
- Create: `src/orchestrator.py`
- Test: `tests/test_orchestrator.py`

**Interfaces:**
- Consumes: `has_seen`/`mark_seen` (Task 14), `is_blacklisted` (Task 10), `passes_employment_filter` (Task 11), `matches_tech_stack` (Task 12), `is_duplicate` (Task 13), `TelegramNotifier` (Task 15).
- Produces: `process_job(conn, job: JobPosting, blacklist: dict, profile: dict, notifier) -> str` (returns one of `"duplicate_seen"`, `"blacklisted"`, `"employment_filtered"`, `"keyword_filtered"`, `"fuzzy_duplicate"`, `"alerted"`) and `resurface_snoozed(conn, notifier) -> int` (returns count re-alerted). Used by `main.py` (Task 18).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_orchestrator.py
from datetime import datetime, timedelta
from src.ingestion.models import JobPosting
from src.orchestrator import process_job, resurface_snoozed

BLACKLIST = {"companies": ["Bad Co"], "keywords": ["unpaid"]}
PROFILE = {
    "tech_stack_keywords": ["python"],
    "employment_preferences": {"accepting_full_time": True, "accepting_part_time": False},
}


class FakeNotifier:
    def __init__(self):
        self.alerts = []

    def send_job_alert(self, job_id, title, company, location, employment_type, source, url):
        self.alerts.append((job_id, title, company))

    def send_health_alert(self, source, last_error):
        pass


def make_job(**overrides):
    defaults = dict(
        source="wwr", source_job_id="1", title="Python Engineer", company="Acme",
        description="", url="http://x", location="Remote",
        employment_type="full_time", salary_raw=None, posted_at=None,
    )
    defaults.update(overrides)
    return JobPosting(**defaults)


def test_alerts_and_inserts_a_clean_job(db_conn):
    notifier = FakeNotifier()
    outcome = process_job(db_conn, make_job(), BLACKLIST, PROFILE, notifier)
    assert outcome == "alerted"
    assert len(notifier.alerts) == 1
    row = db_conn.execute("SELECT status FROM jobs WHERE source_job_id = '1'").fetchone()
    assert row["status"] == "alerted"


def test_drops_already_seen_job_without_reprocessing(db_conn):
    notifier = FakeNotifier()
    process_job(db_conn, make_job(), BLACKLIST, PROFILE, notifier)
    outcome = process_job(db_conn, make_job(), BLACKLIST, PROFILE, notifier)
    assert outcome == "duplicate_seen"
    assert len(notifier.alerts) == 1


def test_drops_blacklisted_job(db_conn):
    notifier = FakeNotifier()
    outcome = process_job(db_conn, make_job(company="Bad Co", source_job_id="2"), BLACKLIST, PROFILE, notifier)
    assert outcome == "blacklisted"
    assert notifier.alerts == []


def test_drops_job_failing_employment_filter(db_conn):
    notifier = FakeNotifier()
    outcome = process_job(
        db_conn, make_job(employment_type="part_time", source_job_id="3"), BLACKLIST, PROFILE, notifier
    )
    assert outcome == "employment_filtered"
    assert notifier.alerts == []


def test_drops_job_failing_keyword_filter(db_conn):
    notifier = FakeNotifier()
    outcome = process_job(
        db_conn, make_job(title="Sales Rep", source_job_id="4"), BLACKLIST, PROFILE, notifier
    )
    assert outcome == "keyword_filtered"
    assert notifier.alerts == []


def test_drops_fuzzy_duplicate_from_other_source(db_conn):
    notifier = FakeNotifier()
    process_job(db_conn, make_job(source_job_id="5"), BLACKLIST, PROFILE, notifier)
    duplicate = make_job(source="remoteok", source_job_id="6", title="Python Engineer, Remote")
    outcome = process_job(db_conn, duplicate, BLACKLIST, PROFILE, notifier)
    assert outcome == "fuzzy_duplicate"
    assert len(notifier.alerts) == 1


def test_resurface_snoozed_realerts_expired_jobs(db_conn):
    old = (datetime.utcnow() - timedelta(hours=25)).isoformat()
    db_conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, url,
                              location, employment_type, fetched_at, status)
           VALUES ('wwr', '9', 'Python Engineer', 'Acme', 'http://x', 'Remote',
                   'unknown', ?, 'snoozed')""",
        (old,),
    )
    db_conn.commit()
    notifier = FakeNotifier()
    count = resurface_snoozed(db_conn, notifier)
    assert count == 1
    assert len(notifier.alerts) == 1
    row = db_conn.execute("SELECT status FROM jobs WHERE source_job_id = '9'").fetchone()
    assert row["status"] == "alerted"


def test_resurface_snoozed_ignores_jobs_within_window(db_conn):
    recent = (datetime.utcnow() - timedelta(hours=1)).isoformat()
    db_conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, url,
                              location, employment_type, fetched_at, status)
           VALUES ('wwr', '10', 'Python Engineer', 'Acme', 'http://x', 'Remote',
                   'unknown', ?, 'snoozed')""",
        (recent,),
    )
    db_conn.commit()
    notifier = FakeNotifier()
    count = resurface_snoozed(db_conn, notifier)
    assert count == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_orchestrator.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.orchestrator'`

- [ ] **Step 3: Implement**

```python
# src/orchestrator.py
import sqlite3
from datetime import datetime, timedelta

from src.filters.seen import has_seen, mark_seen
from src.filters.blacklist import is_blacklisted
from src.filters.employment_type import passes_employment_filter
from src.filters.keywords import matches_tech_stack
from src.filters.dedup import is_duplicate
from src.ingestion.models import JobPosting

SNOOZE_HOURS = 24


def process_job(conn: sqlite3.Connection, job: JobPosting, blacklist: dict,
                 profile: dict, notifier) -> str:
    if has_seen(conn, job.source, job.source_job_id):
        return "duplicate_seen"
    mark_seen(conn, job.source, job.source_job_id)

    if is_blacklisted(job, blacklist):
        return "blacklisted"

    if not passes_employment_filter(job, profile.get("employment_preferences", {})):
        return "employment_filtered"

    if not matches_tech_stack(job, profile.get("tech_stack_keywords", [])):
        return "keyword_filtered"

    if is_duplicate(conn, job):
        return "fuzzy_duplicate"

    now = datetime.utcnow().isoformat()
    cursor = conn.execute(
        """INSERT INTO jobs (source, source_job_id, title, company, description, url,
                              location, employment_type, salary_raw, posted_at, fetched_at, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new')""",
        (
            job.source, job.source_job_id, job.title, job.company, job.description, job.url,
            job.location, job.employment_type, job.salary_raw,
            job.posted_at.isoformat() if job.posted_at else None, now,
        ),
    )
    conn.commit()
    job_id = cursor.lastrowid

    notifier.send_job_alert(job_id, job.title, job.company, job.location,
                             job.employment_type, job.source, job.url)
    conn.execute("UPDATE jobs SET status = 'alerted' WHERE id = ?", (job_id,))
    conn.commit()
    return "alerted"


def resurface_snoozed(conn: sqlite3.Connection, notifier) -> int:
    cutoff = (datetime.utcnow() - timedelta(hours=SNOOZE_HOURS)).isoformat()
    rows = conn.execute(
        "SELECT * FROM jobs WHERE status = 'snoozed' AND fetched_at <= ?", (cutoff,)
    ).fetchall()
    for row in rows:
        notifier.send_job_alert(row["id"], row["title"], row["company"], row["location"],
                                 row["employment_type"], row["source"], row["url"])
        conn.execute(
            "UPDATE jobs SET status = 'alerted', fetched_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), row["id"]),
        )
    conn.commit()
    return len(rows)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_orchestrator.py -v`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add src/orchestrator.py tests/test_orchestrator.py
git commit -m "feat: add orchestrator pipeline and snooze resurfacing"
```

---

### Task 18: Main polling loop

**Files:**
- Create: `main.py`
- Test: `tests/test_main.py`

**Interfaces:**
- Consumes: every adapter class (Tasks 4-9), `record_success`/`record_failure`/`is_disabled`/`should_poll` (Task 3), `process_job`/`resurface_snoozed` (Task 17), `TelegramNotifier` (Task 15), `apply_callback` (Task 16).
- Produces: `run_cycle(conn, sources_config: dict, blacklist: dict, profile: dict, notifier, adapter_classes: dict) -> dict` (one full poll pass, fully testable) and `main()` (infinite loop wrapper, not unit tested — glue only).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_main.py
from main import run_cycle


class FakeAdapter:
    def __init__(self, postings=None, error=None):
        self._postings = postings or []
        self._error = error

    def fetch(self):
        if self._error:
            raise self._error
        return self._postings


class FakeNotifier:
    def __init__(self):
        self.alerts = []
        self.health_alerts = []

    def send_job_alert(self, job_id, title, company, location, employment_type, source, url):
        self.alerts.append(title)

    def send_health_alert(self, source, last_error):
        self.health_alerts.append(source)

    def get_callback_updates(self, offset=None):
        return []


SOURCES_CONFIG = {
    "fake_source": {"enabled": True, "poll_interval_minutes": 30, "delay_range_seconds": [0, 0]}
}
BLACKLIST = {"companies": [], "keywords": []}
PROFILE = {"tech_stack_keywords": [], "employment_preferences": {"accepting_full_time": True, "accepting_part_time": True}}


def make_job(source_job_id):
    from src.ingestion.models import JobPosting
    return JobPosting(
        source="fake_source", source_job_id=source_job_id, title="Engineer", company="Acme",
        description="", url="http://x", location="Remote",
        employment_type="unknown", salary_raw=None, posted_at=None,
    )


def test_run_cycle_processes_enabled_source_and_alerts(db_conn):
    notifier = FakeNotifier()
    adapter_classes = {"fake_source": lambda: FakeAdapter(postings=[make_job("1")])}
    results = run_cycle(db_conn, SOURCES_CONFIG, BLACKLIST, PROFILE, notifier, adapter_classes)
    assert results["fake_source"] == ["alerted"]
    assert notifier.alerts == ["Engineer"]


def test_run_cycle_skips_disabled_source(db_conn):
    from src.ingestion.health import record_failure
    for _ in range(5):
        record_failure(db_conn, "fake_source")
    notifier = FakeNotifier()
    adapter_classes = {"fake_source": lambda: FakeAdapter(postings=[make_job("1")])}
    results = run_cycle(db_conn, SOURCES_CONFIG, BLACKLIST, PROFILE, notifier, adapter_classes)
    assert "fake_source" not in results


SOURCES_CONFIG_NO_COOLDOWN = {
    "fake_source": {"enabled": True, "poll_interval_minutes": 0, "delay_range_seconds": [0, 0]}
}


def test_run_cycle_records_failure_and_sends_health_alert_at_threshold(db_conn):
    # poll_interval_minutes=0 so should_poll never blocks the next attempt in this loop
    notifier = FakeNotifier()
    adapter_classes = {"fake_source": lambda: FakeAdapter(error=RuntimeError("boom"))}
    for _ in range(4):
        run_cycle(db_conn, SOURCES_CONFIG_NO_COOLDOWN, BLACKLIST, PROFILE, notifier, adapter_classes)
    assert notifier.health_alerts == []
    run_cycle(db_conn, SOURCES_CONFIG_NO_COOLDOWN, BLACKLIST, PROFILE, notifier, adapter_classes)
    assert notifier.health_alerts == ["fake_source"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_main.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'main'`

- [ ] **Step 3: Implement `main.py`**

```python
# main.py
import json
import random
import time
import os

from dotenv import load_dotenv

from src.db import get_connection, init_db, DB_PATH
from src.ingestion.health import record_success, record_failure, is_disabled, should_poll
from src.ingestion.we_work_remotely import WeWorkRemotelyAdapter
from src.ingestion.remoteok import RemoteOKAdapter
from src.ingestion.remotive import RemotiveAdapter
from src.ingestion.nodesk import NoDeskAdapter
from src.ingestion.flexjobs import FlexJobsAdapter
from src.ingestion.linkedin import LinkedInAdapter
from src.notifier.telegram import TelegramNotifier
from src.notifier.callbacks import apply_callback
from src.orchestrator import process_job, resurface_snoozed

DEFAULT_ADAPTER_CLASSES = {
    "we_work_remotely": WeWorkRemotelyAdapter,
    "remoteok": RemoteOKAdapter,
    "remotive": RemotiveAdapter,
    "nodesk": NoDeskAdapter,
    "flexjobs": FlexJobsAdapter,
    "linkedin": LinkedInAdapter,
}


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_cycle(conn, sources_config: dict, blacklist: dict, profile: dict, notifier,
              adapter_classes: dict = DEFAULT_ADAPTER_CLASSES) -> dict:
    results = {}
    for source, cfg in sources_config.items():
        if not cfg.get("enabled", False):
            continue
        if is_disabled(conn, source):
            continue
        if not should_poll(conn, source, cfg.get("poll_interval_minutes", 30)):
            continue

        low, high = cfg.get("delay_range_seconds", [1, 3])
        if high > 0:
            time.sleep(random.uniform(low, high))

        adapter = adapter_classes[source]()
        try:
            postings = adapter.fetch()
        except Exception as exc:
            just_disabled = record_failure(conn, source)
            if just_disabled:
                notifier.send_health_alert(source, str(exc))
            continue

        record_success(conn, source)
        results[source] = [
            process_job(conn, job, blacklist, profile, notifier) for job in postings
        ]

    resurface_snoozed(conn, notifier)

    offset = None
    for update in notifier.get_callback_updates(offset):
        callback = update.get("callback_query")
        if callback:
            apply_callback(conn, callback["data"])
            offset = update["update_id"] + 1

    return results


def main():
    load_dotenv()
    conn = get_connection(DB_PATH)
    init_db(conn)
    sources_config = load_json("config/sources.json")
    blacklist = load_json("config/blacklist.json")
    profile = load_json("data/master_profile.json")
    notifier = TelegramNotifier(
        bot_token=os.environ["TELEGRAM_BOT_TOKEN"],
        chat_id=os.environ["TELEGRAM_CHAT_ID"],
    )

    while True:
        run_cycle(conn, sources_config, blacklist, profile, notifier)
        time.sleep(60)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_main.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Run the full test suite**

Run: `pytest -v`
Expected: PASS (all tests across every task)

- [ ] **Step 6: Commit**

```bash
git add main.py tests/test_main.py
git commit -m "feat: add main polling loop wiring adapters, filters, and Telegram"
```

---

## Post-Plan Manual Setup (not automated, do once before running for real)

1. Copy `config/sources.example.json` → `config/sources.json`, `config/blacklist.example.json` → `config/blacklist.json`, `data/master_profile.example.json` → `data/master_profile.json`, and fill in real values (these are gitignored).
2. Create a `.env` file (gitignored) with `TELEGRAM_BOT_TOKEN=...` and `TELEGRAM_CHAT_ID=...`.
3. Generate `data/linkedin_storage_state.json` via a one-time manual Playwright login (see Task 9, Step 6) before enabling the `linkedin` source.
4. Run `playwright install chromium` once to fetch the browser binary Playwright needs.
