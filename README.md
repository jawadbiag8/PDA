# KPI Monitoring System

Automated monitoring system for Pakistani government websites with intelligent target-based evaluation, frequency-based scheduling, and automatic incident management.

## Features

- **24 Automated KPI Checks** across 6 categories
- **Frequency-Based Scheduling** (1 min, 5 min, 15 min, Daily)
- **CitizenImpactLevel-Based Targets** (High/Medium/Low thresholds)
- **Automatic Incident Management** (create after consecutive failures, auto-close on recovery)
- **UPSERT Result Storage** (snapshot + history tables)
- **Hierarchical Asset Management** (Ministries > Departments > Assets)

## Project Structure

```
KPIs-HealthCheck/
├── src/
│   ├── scheduler/
│   │   └── scheduler_v3.py      # Main frequency-based scheduler
│   └── kpi_runners/
│       ├── base.py              # Base runner class
│       ├── http_runner.py       # HTTP/availability checks
│       ├── dns_runner.py        # DNS resolution checks
│       ├── browser_runner.py    # Browser-based checks (Playwright)
│       ├── ssl_runner.py        # SSL certificate checks
│       └── accessiblity_runner.py  # WCAG accessibility checks
├── database/
│   ├── setup.py                 # Database initialization
│   ├── add_unique_constraint.py # Add UPSERT constraint
│   ├── set_citizen_impact_levels.py  # Set asset impact levels
│   └── ...                      # Other migration scripts
├── requirements.txt
├── .gitignore
└── README.md
```

## Prerequisites

- Python 3.10+
- MySQL 8.0+
- Playwright browsers (for accessibility checks)

## Installation

### 1. Clone and Setup Virtual Environment

```bash
git clone <repository-url>
cd KPIs-HealthCheck

# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Linux/Mac)
source venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium
python -m playwright install
python -m playwright install --with-deps
```

### 3. Configure Database

Update database configuration in `src/scheduler/scheduler_v3.py`:

```python
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "kpi_monitoring"
}
```

### 4. Initialize Database

```bash
python database/setup.py
python database/add_unique_constraint.py
python database/set_citizen_impact_levels.py
```

## Usage

### Start the Scheduler

```bash
# Run the scheduler (continuous monitoring)
python src/scheduler/scheduler_v3.py

# Test mode - run all KPIs once
python src/scheduler/scheduler_v3.py --test

# Run specific frequency only
python src/scheduler/scheduler_v3.py --frequency "5 min"
```

### Schedule Configuration

| Frequency | KPIs | Interval |
|-----------|------|----------|
| 1 min | Critical availability checks | Every 1 minute |
| 5 min | Performance monitoring | Every 5 minutes |
| 15 min | Security and SSL checks | Every 15 minutes |
| Daily | Accessibility audits | Daily at 23:30 |

Configure daily run time in `scheduler_v3.py`:

```python
DAILY_RUN_HOUR = 23   # 11 PM
DAILY_RUN_MINUTE = 30
```

## KPI Categories

### 1. Availability & Reliability
- Website completely down (no response)
- DNS failure (domain resolution issue)
- Hosting/network outage
- Partial outage (degraded response)
- Intermittent availability (flapping)

### 2. Performance & Efficiency
- Slow page load
- Backend response time too long
- Heavy pages slowing browsing

### 3. Security, Trust & Privacy
- Website not using HTTPS
- SSL certificate expired/invalid
- Security warnings in browser
- Mixed content warnings
- Redirect issues
- Privacy policy not found

### 4. Accessibility & Inclusivity
- WCAG compliance score
- Missing form labels
- Images without alt text
- Poor color contrast

### 5. User Experience & Journey Quality
- File download failure
- Broken download links
- Broken assets (images, CSS, JS)

### 6. Navigation & Discoverability
- Search not available
- Broken internal links
- Circular navigation issues

## Target Comparison Logic

KPI results are evaluated against targets based on the asset's CitizenImpactLevel:

| Impact Level | Description | Target Column |
|--------------|-------------|---------------|
| High | Critical public services | TargetHigh (strictest) |
| Medium | Standard services | TargetMedium |
| Low | Internal/low-traffic | TargetLow (most relaxed) |

### Comparison Rules by Outcome Type

| Outcome | Logic | Example |
|---------|-------|---------|
| Flag | Runner's flag directly | Site down = miss |
| Sec | Lower is better | Result <= Target = hit |
| MB | Lower is better | Result <= Target = hit |
| % | Higher is better | Result >= Target = hit |

### Example

For "Slow page load" KPI with targets: High=2s, Medium=3s, Low=5s

| Asset Impact | Load Time | Target | Result |
|--------------|-----------|--------|--------|
| High | 2.5s | 2.0s | MISS |
| Medium | 2.5s | 3.0s | HIT |
| Low | 2.5s | 5.0s | HIT |

## Incident Management

### Automatic Creation
- Incidents created after consecutive failures (configured per KPI via `incidentCreationFrequency`)
- Incident Title: `{KpiName} - Breach`
- Incident Description: `{KpiName} - Auto Created Incident`
- Type: `auto`

### Auto-Close
- Only `auto` type incidents are closed automatically
- Closed when KPI check passes (recovers)
- Manual incidents require manual closure

## Database Tables

### Core Tables

| Table | Purpose |
|-------|---------|
| `assets` | Registered websites/applications |
| `KpisLov` | KPI definitions with targets |
| `kpis_results` | Current state (UPSERT - 1 row per Asset+KPI) |
| `kpi_results_history` | Full timeline (INSERT only) |
| `incidents` | Tracked issues |

### Supporting Tables

| Table | Purpose |
|-------|---------|
| `ministries` | Parent organizations |
| `departments` | Sub-organizations |
| `CommonLookup` | Lookup values (CitizenImpactLevel, etc.) |

## Result Storage

### kpis_results (Snapshot)
- One row per Asset+KPI combination
- Uses UPSERT (INSERT ON DUPLICATE KEY UPDATE)
- `createdAt` = first check, `updatedAt` = latest check

### kpi_results_history (Timeline)
- Multiple rows per Asset+KPI (full history)
- Always INSERT (never update)
- Used for consecutive failure detection

### Flag Column Values

| Outcome Type | Flag Value |
|--------------|------------|
| Flag | `"true"` or `"false"` |
| Sec | Numeric seconds (e.g., `"2.5"`) |
| MB | Numeric megabytes (e.g., `"0.75"`) |
| % | Numeric percentage (e.g., `"85.5"`) |

## SQL Queries

### View Current KPI Status

```sql
SELECT
    a.AssetName,
    k.KpiName,
    r.Target as HitMiss,
    r.Result,
    r.updatedAt
FROM kpis_results r
JOIN assets a ON r.AssetId = a.Id
JOIN KpisLov k ON r.KpiId = k.Id
WHERE a.deleted_at IS NULL
ORDER BY a.AssetName, k.KpiGroup;
```

### View Open Incidents

```sql
SELECT
    i.Id,
    i.IncidentTitle,
    a.AssetName,
    i.SeverityLevel,
    i.Status,
    i.createdAt
FROM incidents i
JOIN assets a ON i.AssetId = a.Id
WHERE i.Status = 'Open'
ORDER BY i.createdAt DESC;
```

### Hit/Miss Rate by Impact Level

```sql
SELECT
    cl.Name as ImpactLevel,
    COUNT(*) as TotalChecks,
    SUM(CASE WHEN r.Target = 'hit' THEN 1 ELSE 0 END) as Hits,
    SUM(CASE WHEN r.Target = 'miss' THEN 1 ELSE 0 END) as Misses
FROM kpis_results r
JOIN assets a ON r.AssetId = a.Id
LEFT JOIN CommonLookup cl ON a.CitizenImpactLevelId = cl.Id
GROUP BY cl.Name;
```

## Troubleshooting

### Playwright Issues

```bash
# Reinstall browsers
playwright install chromium --force

# Run with headed browser for debugging
# (modify browser_runner.py: headless=False)
```

### Database Connection

```bash
# Test MySQL connection
mysql -u root -p kpi_monitoring
```

### Missing Dependencies

```bash
pip install -r requirements.txt --upgrade
```

## Development

### Adding a New KPI Runner

1. Create runner in `src/kpi_runners/`
2. Inherit from `BaseKPIRunner`
3. Implement `run()` method returning:

```python
{
    "flag": bool,      # True = problem detected
    "value": any,      # Metric value
    "details": str     # Human-readable description
}
```

4. Register in `get_runner()` in scheduler

### Adding New Frequency

1. Add frequency value to `KpisLov.Frequency` column
2. Create job function in scheduler
3. Add to `start_scheduler()`:

```python
scheduler.add_job(job_new_freq, IntervalTrigger(minutes=X), ...)
```

## License

Internal use only - Pakistan Digital Authority (PDA)
