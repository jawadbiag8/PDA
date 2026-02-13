# KPI Health Check System - Consolidated Documentation

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Project Structure](#2-project-structure)
3. [Environment Configuration](#3-environment-configuration)
4. [Automated Scheduler](#4-automated-scheduler)
5. [Manual KPI API Server](#5-manual-kpi-api-server)
6. [KPI Runners](#6-kpi-runners)
7. [Hit/Miss Target Logic](#7-hitmiss-target-logic)
8. [Incident Management](#8-incident-management)
9. [Asset Metrics Calculation](#9-asset-metrics-calculation)
10. [HEAD/GET Fallback Logic](#10-headget-fallback-logic)
11. [Deployment](#11-deployment)
12. [Troubleshooting](#12-troubleshooting)

---

## 1. Project Overview

The KPI Health Check System monitors government digital assets (websites) against defined Key Performance Indicators. It runs automated checks on configurable schedules and also exposes an API for manual, on-demand KPI checks.

**Two independent processes:**
- **Automated Scheduler** (`scheduler_v3.py`) - Runs KPIs on scheduled intervals using APScheduler
- **Manual API Server** (`server.py`) - FastAPI endpoint for on-demand KPI checks

Both processes share the same KPI runner logic, database, and result storage, but run independently with separate logs.

---

## 2. Project Structure

```
KPIs-HealthCheck/
├── .env                              # Environment configuration
├── requirements.txt                  # Python dependencies
├── scripts/
│   ├── auto.sh                       # Shell script to start/stop automated scheduler
│   └── manual.sh                     # Shell script to start/stop manual API server
├── src/
│   ├── api/
│   │   ├── __init__.py
│   │   └── server.py                 # FastAPI manual KPI trigger server
│   ├── config/
│   │   └── settings.py               # DB config, timeouts, retry settings
│   ├── kpi_runners/
│   │   ├── base.py                   # Base KPI runner class
│   │   ├── http_runner.py            # HTTP-based KPIs (down check, response time, etc.)
│   │   ├── dns_runner.py             # DNS resolution KPIs
│   │   ├── ssl_runner.py             # SSL certificate KPIs
│   │   ├── browser_runner.py         # Browser-based KPIs (page load, broken links, etc.)
│   │   └── accessiblity_runner.py    # Accessibility KPIs (WCAG compliance)
│   └── scheduler/
│       └── scheduler_v3.py           # Main scheduler + all shared business logic
├── database/                         # Migration scripts
└── documents/                        # Documentation
```

---

## 3. Environment Configuration

File: `.env`

| Variable | Description | Example |
|---|---|---|
| `DB_HOST` | MySQL database host | `47.129.240.107` |
| `DB_NAME` | Database name | `appdb_qa` |
| `DB_USER` | Database user | `appuser` |
| `DB_PASS` | Database password | `mm@001` |
| `DAILY_RUN_HOUR` | Hour for daily KPI run (24h) | `15` |
| `DAILY_RUN_MINUTE` | Minute for daily KPI run | `40` |
| `PARALLEL_WORKERS` | Number of parallel threads | `5` |
| `AUTO_LOG_PATH` | Log directory for automated scheduler | `/var/logs/kpiAutomationLogs/` |
| `MANUAL_LOG_PATH` | Log directory for manual API server | `/var/logs/kpiManualLogs/` |
| `API_HOST` | Manual API server bind address | `0.0.0.0` |
| `API_PORT` | Manual API server port | `8000` |

**Timeouts** (in `src/config/settings.py`):
| Setting | Value | Description |
|---|---|---|
| `DEFAULT_TIMEOUT` | 20 seconds | HTTP request timeout |
| `FLAPPING_TIMEOUT` | 5 seconds | Intermittent availability check timeout |
| `RETRY_DELAY` | 3 seconds | Wait between retry attempts |

---

## 4. Automated Scheduler

File: `src/scheduler/scheduler_v3.py`

Uses APScheduler `BlockingScheduler` to run KPI checks at configured intervals.

### Schedule Frequencies

| Frequency | Actual Interval | Misfire Grace | KPIs |
|---|---|---|---|
| 1 min | Every 3 minutes | 60s | KPI 1 (completely down) + other 1-min KPIs |
| 5 min | Every 10 minutes | 120s | KPIs marked with Frequency = '5 min' |
| 15 min | Every 20 minutes | 180s | KPIs marked with Frequency = '15 min' |
| Daily | Configurable (cron) | 300s | KPIs marked with Frequency = 'Daily' |

**All jobs:** `coalesce=True` (missed runs only fire once), `max_instances=1`

### Processing Flow Per Frequency

**1-min job** (no pre-check):
1. Fetch all non-deleted assets
2. Fetch KPIs with `Frequency = '1 min'` AND `Manual = 'Auto'`
3. Process assets in parallel using `ThreadPoolExecutor`
4. KPI 1 ("completely down") runs first. If it's a miss AND 2 consecutive failures exist, remaining KPIs for that asset are skipped

**5-min / 15-min / Daily jobs** (with pre-check):
1. Fetch all non-deleted assets
2. **Pre-check**: HEAD request (fallback to GET) to see if site is reachable
3. If site is down, skip all KPIs for that asset (mark as "skipped")
4. If site is up, run each KPI through its runner

### Parallel Processing
- Uses `ThreadPoolExecutor` with `PARALLEL_WORKERS` threads
- Each thread gets its own DB connection (`get_thread_db_connection()`)
- Logs are buffered per thread and flushed in order for clean output

### Start/Stop Commands

```bash
python src/scheduler/scheduler_v3.py --start      # Start as background daemon
python src/scheduler/scheduler_v3.py --stop       # Stop running scheduler
python src/scheduler/scheduler_v3.py --test       # Run all KPIs once (test mode)
python src/scheduler/scheduler_v3.py --frequency "1 min"   # Run specific frequency once
python src/scheduler/scheduler_v3.py               # Run in foreground
```

PID file: `{AUTO_LOG_PATH}/scheduler.pid`

### Logging
- Date-wise log files: `{AUTO_LOG_PATH}/YYYY-MM-DD.log`
- Rotates automatically at midnight

---

## 5. Manual KPI API Server

File: `src/api/server.py`

FastAPI server that exposes an endpoint for manually triggering a specific KPI check for a specific asset.

### Architecture
```
Frontend  -->  .NET Backend  -->  KPI API Server (port 8000)  -->  Background Thread
                                      |                                    |
                                  Returns immediately            Runs full KPI logic
                                  with estimated time            (results, incidents, metrics)
```

### Endpoint

**POST** `/api/kpi/manual-check`

**Request:**
```json
{
  "kpiId": 1,
  "assetId": 235
}
```

**Response (immediate, fire-and-forget):**
```json
{
  "success": true,
  "message": "It will take approximately 5-10 seconds to complete. Please check the results shortly.",
  "data": {
    "kpiId": 1,
    "kpiName": "Website completely down (no response)",
    "assetId": 235,
    "assetName": "Website - Ministry of Defence Production",
    "assetUrl": "https://modp.gov.pk",
    "estimatedTime": "5-10 seconds"
  }
}
```

**Estimated times by KPI type:**
| KPI Type | Estimated Time |
|---|---|
| http | 5-10 seconds |
| dns | 5-10 seconds |
| ssl | 10-15 seconds |
| browser | 15-20 seconds |
| accessibility | 20-30 seconds |

### Background Processing
The KPI runs in a daemon thread with full logic:
- Stores result in `kpisResults` (UPSERT)
- Stores history in `KPIsResultHistories` (INSERT)
- Creates/closes incidents based on consecutive failures/hits
- Recalculates asset metrics

### Start/Stop Commands

```bash
python src/api/server.py --start     # Start as background daemon
python src/api/server.py --stop      # Stop running server
```

PID file: `{MANUAL_LOG_PATH}/api_server.pid`
Fallback stop: If PID file is missing, uses `lsof -ti :PORT` to find the process

### Logging
- Separate from scheduler logs (uses `manual_kpi` logger with `propagate=False`)
- Date-wise log files: `{MANUAL_LOG_PATH}/YYYY-MM-DD.log`
- Log entries prefixed with `[MANUAL]`

### Curl Example for Direct Testing
```bash
curl --location 'http://localhost:8000/api/kpi/manual-check' \
  --header 'Content-Type: application/json' \
  --data '{"kpiId": 1, "assetId": 235}'
```

---

## 6. KPI Runners

Each KPI has a `KpiType` in the `KpisLov` table that maps to a runner:

| KpiType | Runner | Description |
|---|---|---|
| `http` | `HttpKPIRunner` | HTTP-based checks (site down, response time, page size, etc.) |
| `dns` | `DnsKPIRunner` | DNS resolution checks |
| `ssl` | `SslKPIRunner` | SSL certificate validation |
| `browser` | `SharedBrowserContext` | Browser-based checks using Playwright (page load, broken links, etc.) |
| `accessibility` | `AccessibilityRunner` | WCAG accessibility checks using Playwright |

### Runner Return Format
Every runner returns:
```python
{
    "flag": True/False,    # True = problem detected, False = no problem
    "value": <number>,     # Measured value (response time, score, etc.)
    "details": "string"   # Human-readable details
}
```

### HTTP Runner - KPI 1 ("completely down") Check Flow
1. **HEAD request** (attempt 1) - timeout: 20s
2. If fails, wait 3s, **HEAD request** (attempt 2) - timeout: 20s
3. If both HEAD fail, **GET request** (fallback) - timeout: 20s
4. If all fail → site is DOWN (`flag: True`)

---

## 7. Hit/Miss Target Logic

File: `scheduler_v3.py` → `determine_target_hit_miss()`

### Target Selection
Each KPI has three target thresholds. The one used depends on the asset's `CitizenImpactLevel`:
- **High** impact → `TargetHigh` (strictest)
- **Medium** impact → `TargetMedium`
- **Low** impact → `TargetLow` (most lenient)

### Comparison Rules

| Outcome Type | Hit Condition | Use Case |
|---|---|---|
| **Flag** | `flag = false` | Binary checks (site up/down) |
| **Sec** | `result <= target` | Response time (lower is better) |
| **MB** | `result <= target` | Page size (lower is better) |
| **%** | `result >= target` | Scores/percentages (higher is better) |

**Important:** Equal to target (`=`) is always a **hit**, not a miss.

**Note:** Target values in the database may contain unit suffixes (e.g., `"1%"`, `"5s"`). The system strips non-numeric characters before comparison to avoid parsing errors.

### Result Storage

**`kpisResults` table** (UPSERT - one row per asset+KPI):
```sql
INSERT INTO kpisResults (AssetId, KpiId, Result, Details, CreatedAt, UpdatedAt, Target)
VALUES (...)
ON DUPLICATE KEY UPDATE Result=..., Details=..., UpdatedAt=NOW(), Target=...
```
- First insert: both `CreatedAt` and `UpdatedAt` are set to `NOW()`
- Subsequent updates: only `UpdatedAt` changes

**`KPIsResultHistories` table** (INSERT - one row per check):
```sql
INSERT INTO KPIsResultHistories (AssetId, KPIsResultId, KpiId, Details, CreatedAt, Target, Result)
VALUES (...)
```

---

## 8. Incident Management

### Incident Creation Flow

1. KPI check runs → result is **miss**
2. System queries `KPIsResultHistories` for the last N results (N = `IncidentCreationFrequency` from `CommonLookup`, default: 3)
3. **Only if ALL N most recent results are consecutive misses** → create incident
4. Before creating, checks if an open incident already exists for that asset+KPI (`StatusId = 8`)
   - If exists → skip (log as "already open")
   - If not → INSERT into `Incidents` table

**Incident record:**
- `Type = 'auto'`
- `StatusId = 8` (Open)
- `SeverityId` = from the KPI's `SeverityId` field
- Also creates entries in `IncidentHistories` and `IncidentComments`

### Incident Auto-Close Flow

1. KPI check runs → result is **hit**
2. System checks last N consecutive results
3. **If ALL N are consecutive hits** → auto-close open `Type = 'auto'` incidents
4. Sets `StatusId = 12` (Resolved)
5. Creates entries in `IncidentHistories` and `IncidentComments`

**Note:** Only `Type = 'auto'` incidents are auto-closed. Manually created incidents are never auto-closed.

---

## 9. Asset Metrics Calculation

File: `scheduler_v3.py` → `recalculate_asset_metrics()`

Called after each asset's KPI cycle completes. Uses **last 30 days** of data from `KPIsResultHistories`.

### Step 1: KPI Group Indexes (6 indexes, each 0-100)

For each KPI: `hit_rate = (hits / total) * 100`

KPIs are grouped by `KpiGroup` from `KpisLov`. Each KPI has a `Weight`. Group score = weighted average of hit rates within the group.

| Index | KPI Group |
|---|---|
| `AccessibilityIndex` | Accessibility & Inclusivity |
| `AvailabilityIndex` | Availability & Reliability |
| `NavigationIndex` | Navigation & Discoverability |
| `PerformanceIndex` | Performance & Efficiency |
| `SecurityIndex` | Security, Trust & Privacy |
| `UserExperienceIndex` | User Experience & Journey Quality |

### Step 2: CHM (Citizen Happiness Metric)
- Weighted average of group scores
- Weights from `MetricWeights` table where `Category = 'CHM'`

### Step 3: OCM (Overall Compliance Metric)
- Weighted average of group scores
- Weights from `MetricWeights` table where `Category = 'OCM'`

### Step 4: DREI (Digital Risk Exposure Index)
- **Open incident ratios** by severity (P1-P4) from `Incidents` table
  - For each severity: `ratio = (open / total) * 100`
  - Each weighted by `MetricWeights` where `Category = 'DREI'` (OpenCritical, OpenHigh, OpenMedium, OpenLow)
- **SLA breach %** = `(total_misses / total_checks) * 100` from last 30 days
  - Weighted by `SLABreach` weight from `MetricWeights`
- **Asset criticality multiplier**: `DREI = raw_drei * (criticality% / 100)`
  - Criticality from `MetricWeights` where `Category = 'AssetCriticality'`

### Step 5: Current Health
```
CurrentHealth = (OCM + (100 - DREI)) / 2
```

### Example: Fully Down Asset
| Metric | Value | Reason |
|---|---|---|
| All 6 Indexes | 0 | 0% hit rate |
| CHM | 0 | Weighted avg of zeros |
| OCM | 0 | Weighted avg of zeros |
| DREI | Depends on incidents & weights | SLA breach = 100% |
| CurrentHealth | `(0 + (100 - DREI)) / 2` | |

### Storage
UPSERT into `AssetMetrics` table (one row per asset, keyed by `AssetId`).

---

## 10. HEAD/GET Fallback Logic

Some government servers (e.g., `cpims.mohr.gov.pk`) reject HEAD requests, returning HTTP 000. To handle this:

### In HTTP Runner (KPI 1 - "completely down"):
1. Try HEAD (2 attempts with retry delay)
2. If HEAD fails → try GET as fallback
3. Details field shows `(HEAD)` or `(GET fallback)` to indicate which method succeeded

### In Pre-checks (5-min, 15-min, Daily schedulers):
1. Try HEAD (timeout: 10s)
2. If HEAD fails or returns status >= 500 → try GET as fallback (timeout: 10s)
3. If both fail → mark site as down, skip all KPIs for that asset

---

## 11. Deployment

### Prerequisites
- Python 3.8+
- MySQL database
- Linux server (production)
- Playwright browsers installed: `playwright install chromium`

### Installation
```bash
cd /opt/PDA
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### Configure `.env`
```bash
# Update with production values
nano .env
```

Ensure `AUTO_LOG_PATH` and `MANUAL_LOG_PATH` point to the correct Linux paths (e.g., `/var/logs/kpiAutomationLogs/`, `/var/logs/kpiManualLogs/`).

### Shell Scripts

**Start everything:**
```bash
./scripts/manual.sh --start    # Start manual API server
./scripts/auto.sh --start      # Start automated scheduler
```

**Stop everything:**
```bash
./scripts/auto.sh --stop       # Stop automated scheduler
./scripts/manual.sh --stop     # Stop manual API server
```

Both scripts:
- Require root
- Create log directories if missing
- Activate the virtualenv
- Delegate to the Python `--start`/`--stop` commands

### Verify Running

```bash
# Check scheduler
ps aux | grep scheduler_v3

# Check API server
curl http://localhost:8000/api/kpi/manual-check -X POST \
  -H "Content-Type: application/json" \
  -d '{"kpiId": 1, "assetId": 235}'

# Check logs
tail -f /var/logs/kpiAutomationLogs/$(date +%Y-%m-%d).log
tail -f /var/logs/kpiManualLogs/$(date +%Y-%m-%d).log
```

---

## 12. Troubleshooting

### API server shows "not running" but curl still works
The server was started outside of the `--start` mechanism (no PID file). The stop command now falls back to port-based detection (`lsof`), but if running old code:
```bash
kill $(lsof -t -i :8000)
```
Then restart with `./scripts/manual.sh --start`.

### Manual logs appearing in auto log files
Fixed by setting `logger.propagate = False` on the `manual_kpi` logger. Ensure you're running the latest code.

### Logs not appearing for manual checks
1. Check you're looking at the correct path (the one printed on `--start`, not an old path)
2. The background thread may be crashing — check for `[TRACEBACK]` entries in the log

### Site falsely reported as "completely down"
Some servers reject HEAD requests. The system now retries HEAD twice, then falls back to GET. Check the `details` field for `(HEAD)` vs `(GET fallback)`.

### KPI showing "miss" when result equals target
Fixed by stripping non-numeric characters (e.g., `%`, `s`) from target values before comparison. `float("1%")` was throwing a ValueError, causing fallback to flag-based logic.

### Asset with DeletedAt value still being checked
All asset queries filter with `WHERE a.DeletedAt IS NULL`. Ensure you're running the latest code.

---

## Database Tables Reference

| Table | Purpose |
|---|---|
| `Assets` | Digital assets (websites) to monitor |
| `KpisLov` | KPI definitions (name, type, targets, frequency, severity) |
| `kpisResults` | Latest KPI result per asset+KPI (UPSERT) |
| `KPIsResultHistories` | Historical KPI results (INSERT per check) |
| `Incidents` | Incident records (auto-created on consecutive failures) |
| `IncidentHistories` | Incident audit trail |
| `IncidentComments` | Incident status comments |
| `AssetMetrics` | Calculated metrics per asset (UPSERT) |
| `MetricWeights` | Weight configuration for CHM, OCM, DREI calculations |
| `CommonLookup` | System configuration (IncidentCreationFrequency, SeverityLevel, etc.) |
| `Ministries` | Ministry reference data |
| `Departments` | Department reference data |
