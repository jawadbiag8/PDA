"""
Frequency-Based KPI Scheduler v3
Runs KPIs at their defined intervals based on the Frequency column in KpisLov table.

Frequencies supported:
- "1 min" - runs every 1 minute
- "5 min" - runs every 5 minutes
- "15 min" - runs every 15 minutes
- "Daily" - runs once daily at DAILY_RUN_TIME (configurable)
"""

import sys
import os

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.kpi_runners.http_runner import HttpKPIRunner
from src.kpi_runners.browser_runner import BrowserKPIRunner
from src.kpi_runners.ssl_runner import SSLKPIRunner
from src.kpi_runners.dns_runner import DNSKPIRunner
from src.kpi_runners.accessiblity_runner import AccessibilityKPIRunner
import mysql.connector
import certifi
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

load_dotenv()
os.environ['SSL_CERT_FILE'] = certifi.where()

# ============================================================
# CONFIGURATION
# ============================================================

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),
    "database": os.getenv("DB_NAME", "kpi_monitoring")
}

# Daily KPIs run time (24-hour format)
DAILY_RUN_HOUR = 15  # 3 PM
DAILY_RUN_MINUTE = 00  # Start of hour

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_db_connection():
    """Get a new database connection"""
    return mysql.connector.connect(**DB_CONFIG)

def get_runner(kpi_type, asset, kpi):
    """Get the appropriate KPI runner based on type"""
    runners = {
        'http': HttpKPIRunner,
        'dns': DNSKPIRunner,
        'browser': BrowserKPIRunner,
        'ssl': SSLKPIRunner,
        'accessibility': AccessibilityKPIRunner
    }
    runner_class = runners.get(kpi_type)
    if runner_class:
        return runner_class(asset, kpi)
    return None

def format_flag_value(result, outcome_type):
    """
    Format the Flag value based on outcome type.
    Used for display purposes only (no Flag column in DB).
    """
    value = result.get('value')
    flag = result.get('flag')

    if outcome_type == 'Flag':
        return 'true' if flag else 'false'
    elif outcome_type == 'Sec':
        return str(value) if value is not None else '0'
    elif outcome_type == 'MB':
        return str(value) if value is not None else '0'
    elif outcome_type == '%':
        return str(value) if value is not None else '0'
    else:
        return str(value) if value is not None else ''

def format_result_value(result, outcome_type):
    """Format the Result column value based on outcome type"""
    value = result.get('value')

    if outcome_type == 'Flag':
        return 'true' if result.get('flag') else 'false'
    elif outcome_type == 'Sec':
        return str(value) if value is not None else '0'
    elif outcome_type == 'MB':
        return str(value) if value is not None else '0'
    elif outcome_type == '%':
        return f"{value}%" if value is not None else '0%'
    else:
        return str(value) if value is not None else ''

def determine_target_hit_miss(result_value, target_value, outcome_type, runner_flag):
    """Determine if result is a hit or miss based on target comparison."""
    if outcome_type == 'Flag':
        return "miss" if runner_flag else "hit"

    if not target_value or target_value == '':
        return "miss" if runner_flag else "hit"

    try:
        result_num = float(result_value) if result_value else 0
        target_num = float(target_value)

        if outcome_type in ['Sec', 'MB']:
            return "hit" if result_num <= target_num else "miss"
        elif outcome_type == '%':
            return "hit" if result_num >= target_num else "miss"
        else:
            return "miss" if runner_flag else "hit"

    except (ValueError, TypeError):
        return "miss" if runner_flag else "hit"

def check_consecutive_hits(cursor, asset_id, kpi_id, required_frequency):
    """Check if KPI has passed consecutively for the required number of times"""
    try:
        cursor.execute("""
            SELECT Target
            FROM KPIsResultHistories
            WHERE AssetId = %s AND KpiId = %s
            ORDER BY CreatedAt DESC
            LIMIT %s
        """, (asset_id, kpi_id, required_frequency))

        recent_results = cursor.fetchall()

        if len(recent_results) < required_frequency:
            return False

        return all(record['Target'] == 'hit' for record in recent_results)
    except Exception as e:
        print(f"      [ERROR] Checking consecutive hits: {str(e)}")
        return False

def auto_close_incident(cursor, asset_id, kpi_id):
    """Auto-close incidents when a KPI check passes (only for auto-type incidents)"""
    try:
        cursor.execute("""
            SELECT Id, AssetId, KpiId, IncidentTitle, Description,
                   Type, SeverityId, AssignedTo
            FROM Incidents
            WHERE AssetId = %s AND KpiId = %s AND Status = 'Open' AND Type = 'auto'
        """, (asset_id, kpi_id))

        incidents = cursor.fetchall()
        closed_count = 0

        for incident in incidents:
            cursor.execute("""
                UPDATE Incidents
                SET Status = 'Resolved', UpdatedAt = NOW(), UpdatedBy = 'system'
                WHERE Id = %s
            """, (incident['Id'],))

            # Insert into IncidentHistories (audit trail)
            cursor.execute("""
                INSERT INTO IncidentHistories (AssetId, IncidentId, KpiId, IncidentTitle, Description,
                                                Type, SeverityId, Status, AssignedTo, CreatedBy, CreatedAt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'Resolved', %s, 'system', NOW())
            """, (incident['AssetId'], incident['Id'], incident['KpiId'], incident['IncidentTitle'],
                  incident['Description'], incident['Type'], incident['SeverityId'], incident['AssignedTo']))

            closed_count += 1
            print(f"      [AUTO-CLOSE] Incident #{incident['Id']} resolved")

        return closed_count
    except Exception as e:
        print(f"      [ERROR] Auto-closing incident: {str(e)}")
        return 0

def store_in_results_history(cursor, asset_id, kpis_result_id, kpi_id, target, result_value, details):
    """Store KPI result in history table (KPIsResultHistories)"""
    try:
        if not kpis_result_id:
            print(f"      [WARN] No kpisResults ID, skipping history insert")
            return None

        cursor.execute("""
            INSERT INTO KPIsResultHistories (AssetId, KPIsResultId, KpiId, Details, CreatedAt, Target, Result)
            VALUES (%s, %s, %s, %s, NOW(), %s, %s)
        """, (asset_id, kpis_result_id, kpi_id, details, target, result_value))
        return cursor.lastrowid
    except Exception as e:
        print(f"      [ERROR] Storing in history: {str(e)}")
        return None

def check_consecutive_failures(cursor, asset_id, kpi_id, required_frequency):
    """Check if KPI has failed consecutively for the required number of times"""
    try:
        cursor.execute("""
            SELECT Target
            FROM KPIsResultHistories
            WHERE AssetId = %s AND KpiId = %s
            ORDER BY CreatedAt DESC
            LIMIT %s
        """, (asset_id, kpi_id, required_frequency))

        recent_results = cursor.fetchall()

        if len(recent_results) < required_frequency:
            return False

        return all(record['Target'] == 'miss' for record in recent_results)
    except Exception as e:
        print(f"      [ERROR] Checking consecutive failures: {str(e)}")
        return False

def create_incident(cursor, asset_id, kpi_id, kpi_name, severity_id):
    """Create an incident when a KPI check fails"""
    try:
        cursor.execute("""
            SELECT Id FROM Incidents
            WHERE AssetId = %s AND KpiId = %s AND Status = 'Open'
            LIMIT 1
        """, (asset_id, kpi_id))

        existing_incident = cursor.fetchone()

        if existing_incident:
            return existing_incident['Id'], False

        incident_title = f"{kpi_name} - Breach"
        description = f"{kpi_name} - Auto Created Incident"

        cursor.execute("""
            INSERT INTO Incidents (AssetId, KpiId, IncidentTitle, Description,
                                   Type, SeverityId, Status, AssignedTo, CreatedBy, CreatedAt)
            VALUES (%s, %s, %s, %s, 'auto', %s, 'Open', 'pda@dams.com', 'system', NOW())
        """, (asset_id, kpi_id, incident_title, description, severity_id))

        incident_id = cursor.lastrowid

        # Insert into IncidentHistories (audit trail)
        cursor.execute("""
            INSERT INTO IncidentHistories (AssetId, IncidentId, KpiId, IncidentTitle, Description,
                                            Type, SeverityId, Status, AssignedTo, CreatedBy, CreatedAt)
            VALUES (%s, %s, %s, %s, %s, 'auto', %s, 'Open', 'pda@dams.com', 'system', NOW())
        """, (asset_id, incident_id, kpi_id, incident_title, description, severity_id))

        return incident_id, True
    except Exception as e:
        print(f"      [ERROR] Creating incident: {str(e)}")
        return None, False

def store_result(cursor, asset_id, kpi_id, result, outcome_type, target_value=None, target_override=None):
    """Store KPI result in the database using UPSERT logic. Returns the kpisResults row ID."""
    try:
        result_value = format_result_value(result, outcome_type)
        details = result.get('details', '')

        # Use target_override if provided (e.g., "skipped"), otherwise calculate
        if target_override:
            target = target_override
        else:
            target = determine_target_hit_miss(
                result.get('value'),
                target_value,
                outcome_type,
                result.get('flag')
            )

        cursor.execute("""
            INSERT INTO kpisResults (AssetId, KpiId, Result, Details, CreatedAt, Target)
            VALUES (%s, %s, %s, %s, NOW(), %s)
            ON DUPLICATE KEY UPDATE
                Result = VALUES(Result),
                Details = VALUES(Details),
                UpdatedAt = NOW(),
                Target = VALUES(Target),
                Id = LAST_INSERT_ID(Id)
        """, (asset_id, kpi_id, result_value, details, target))

        return cursor.lastrowid
    except Exception as e:
        print(f"      [ERROR] Storing result: {str(e)}")
        return None

# ============================================================
# KPI EXECUTION
# ============================================================

def run_kpi_for_asset(cursor, asset, kpi, incident_frequency):
    """Run a single KPI check for a single asset"""
    kpi_type = kpi['KpiType']
    kpi_name = kpi['KpiName']
    outcome_type = kpi['Outcome']

    asset_data = {
        'id': asset['Id'],
        'app_name': asset['AssetName'],
        'url': asset['AssetUrl']
    }

    kpi_data = {
        'id': kpi['Id'],
        'kpi_name': kpi_name,
        'kpi_type': kpi_type
    }

    runner = get_runner(kpi_type, asset_data, kpi_data)
    if not runner:
        print(f"      [SKIP] Unknown KPI type: {kpi_type}")
        return None

    try:
        result = runner.run()

        # Determine target based on CitizenImpactLevel (prefix matching for CommonLookup values)
        citizen_impact_level = (asset.get('CitizenImpactLevel') or '').upper()
        if citizen_impact_level.startswith('HIGH'):
            target_value = kpi.get('TargetHigh')
        elif citizen_impact_level.startswith('LOW'):
            target_value = kpi.get('TargetLow')
        else:
            target_value = kpi.get('TargetMedium')

        # Store result (returns kpisResults row ID for history FK)
        result_id = store_result(cursor, asset['Id'], kpi['Id'], result, outcome_type, target_value)

        # Determine hit/miss
        target = determine_target_hit_miss(
            result.get('value'),
            target_value,
            outcome_type,
            result.get('flag')
        )

        # Store in history
        result_value = format_result_value(result, outcome_type)
        store_in_results_history(cursor, asset['Id'], result_id, kpi['Id'], target, result_value, result.get('details', ''))

        # Handle incidents using global incidentCreationFrequency
        if target == "miss":
            should_create = check_consecutive_failures(cursor, asset['Id'], kpi['Id'], incident_frequency)

            if should_create:
                severity_id = kpi.get('SeverityId')
                incident_id, is_new = create_incident(cursor, asset['Id'], kpi['Id'], kpi_name, severity_id)
                if incident_id and is_new:
                    print(f"      [INCIDENT] #{incident_id} created (after {incident_frequency} consecutive misses)")
                elif incident_id:
                    print(f"      [EXISTING] Incident #{incident_id} already open")
            else:
                print(f"      [WAIT] Need {incident_frequency} consecutive misses")
        else:
            # Auto-close only after consecutive hits
            should_close = check_consecutive_hits(cursor, asset['Id'], kpi['Id'], incident_frequency)
            if should_close:
                auto_close_incident(cursor, asset['Id'], kpi['Id'])

        return target

    except Exception as e:
        print(f"      [ERROR] {str(e)}")

        # Store error as skipped
        error_result = {'flag': True, 'value': None, 'details': f"Error: {str(e)[:200]}"}
        result_id = store_result(cursor, asset['Id'], kpi['Id'], error_result, outcome_type, target_override="skipped")

        # Store in history as skipped
        result_value = format_result_value(error_result, outcome_type)
        store_in_results_history(cursor, asset['Id'], result_id, kpi['Id'], "skipped", result_value, f"Error: {str(e)[:200]}")

        return "skipped"

def run_kpis_by_frequency(frequency_filter):
    """Run all KPIs that match the given frequency"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'='*80}")
    print(f"[{timestamp}] Running KPIs with frequency: {frequency_filter}")
    print(f"{'='*80}")

    try:
        # Get global IncidentCreationFrequency from CommonLookup
        cursor.execute("""
            SELECT Name FROM CommonLookup WHERE Type = 'IncidentCreationFrequency' LIMIT 1
        """)
        freq_row = cursor.fetchone()
        incident_frequency = int(freq_row['Name']) if freq_row else 3

        # Get all active assets
        cursor.execute("""
            SELECT a.*, m.MinistryName, d.DepartmentName,
                   cl.Name as CitizenImpactLevel
            FROM Assets a
            LEFT JOIN Ministries m ON a.MinistryId = m.Id
            LEFT JOIN Departments d ON a.DepartmentId = d.Id
            LEFT JOIN CommonLookup cl ON a.CitizenImpactLevelId = cl.Id
            WHERE a.DeletedAt IS NULL
        """)
        assets = cursor.fetchall()

        # Get KPIs matching the frequency (only automated, include SeverityId)
        cursor.execute("""
            SELECT Id, KpiName, KpiGroup, KpiType, Outcome,
                   TargetHigh, TargetMedium, TargetLow, Frequency, SeverityId
            FROM KpisLov
            WHERE KpiType IS NOT NULL AND Frequency = %s
                  AND `Manual` = 'Auto' AND DeletedAt IS NULL
        """, (frequency_filter,))
        kpis = cursor.fetchall()

        if not kpis:
            print(f"  No KPIs found with frequency: {frequency_filter}")
            return

        print(f"  Assets: {len(assets)} | KPIs: {len(kpis)}")

        total_checks = 0
        total_hits = 0
        total_misses = 0
        total_skipped = 0

        for asset in assets:
            print(f"\n  Asset: {asset['AssetName']} ({asset['CitizenImpactLevel'] or 'N/A'})")
            site_is_down = False

            for kpi in kpis:
                kpi_name_lower = kpi['KpiName'].lower()

                # If site is down, store skipped result and move to next KPI
                if site_is_down:
                    total_skipped += 1
                    skipped_result = {'flag': True, 'value': None, 'details': 'Skipped - site is down'}
                    result_id = store_result(cursor, asset['Id'], kpi['Id'], skipped_result, kpi['Outcome'], target_override="skipped")
                    result_value = format_result_value(skipped_result, kpi['Outcome'])
                    store_in_results_history(cursor, asset['Id'], result_id, kpi['Id'], "skipped", result_value, 'Skipped - site is down')
                    print(f"    [SKIP] {kpi['KpiName']} (site is down)")
                    continue

                total_checks += 1
                result = run_kpi_for_asset(cursor, asset, kpi, incident_frequency)

                if result == "hit":
                    total_hits += 1
                    symbol = "[HIT]"
                elif result == "miss":
                    total_misses += 1
                    symbol = "[MISS]"
                elif result == "skipped":
                    total_skipped += 1
                    symbol = "[SKIP]"
                else:
                    symbol = "[ERR]"

                print(f"    {symbol} {kpi['KpiName']}")

                # Check if this was the "site completely down" KPI and it missed
                if 'completely down' in kpi_name_lower and result == "miss":
                    site_is_down = True
                    print(f"    >> Site is DOWN - skipping remaining KPIs for this asset")

            conn.commit()

        print(f"\n  Summary: {total_checks} checks | {total_hits} hits | {total_misses} misses | {total_skipped} skipped")

    except Exception as e:
        print(f"  [ERROR] {str(e)}")
    finally:
        cursor.close()
        conn.close()

# ============================================================
# SCHEDULER JOBS
# ============================================================

def job_1_minute():
    """Job for 1-minute frequency KPIs"""
    run_kpis_by_frequency("1 min")

def job_5_minute():
    """Job for 5-minute frequency KPIs"""
    run_kpis_by_frequency("5 min")

def job_15_minute():
    """Job for 15-minute frequency KPIs"""
    run_kpis_by_frequency("15 min")

def job_daily():
    """Job for daily frequency KPIs"""
    run_kpis_by_frequency("Daily")

# ============================================================
# MAIN
# ============================================================

def run_all_now():
    """Run all KPIs immediately (for testing)"""
    print("\n" + "="*80)
    print("RUNNING ALL KPIs (TEST MODE)")
    print("="*80)

    frequencies = ["1 min", "5 min", "15 min", "Daily"]
    for freq in frequencies:
        run_kpis_by_frequency(freq)

    print("\n" + "="*80)
    print("ALL KPIs COMPLETED")
    print("="*80)

def start_scheduler():
    """Start the scheduler with all jobs"""
    scheduler = BlockingScheduler()

    print("\n" + "="*80)
    print("KPI MONITORING SCHEDULER v3")
    print("="*80)
    print(f"\nSchedule Configuration:")
    print(f"  - 1 min KPIs: Every 1 minute")
    print(f"  - 5 min KPIs: Every 5 minutes")
    print(f"  - 15 min KPIs: Every 15 minutes")
    print(f"  - Daily KPIs: Every day at {DAILY_RUN_HOUR:02d}:{DAILY_RUN_MINUTE:02d}")
    print(f"\nPress Ctrl+C to stop the scheduler.\n")

    # Schedule jobs
    scheduler.add_job(job_1_minute, IntervalTrigger(minutes=1), id='kpi_1min', name='1-minute KPIs')
    scheduler.add_job(job_5_minute, IntervalTrigger(minutes=5), id='kpi_5min', name='5-minute KPIs')
    scheduler.add_job(job_15_minute, IntervalTrigger(minutes=15), id='kpi_15min', name='15-minute KPIs')
    scheduler.add_job(job_daily, CronTrigger(hour=DAILY_RUN_HOUR, minute=DAILY_RUN_MINUTE), id='kpi_daily', name='Daily KPIs')

    # Run 1-minute KPIs immediately on startup
    print("Running initial checks...")
    job_1_minute()

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\nScheduler stopped.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='KPI Monitoring Scheduler v3')
    parser.add_argument('--test', action='store_true', help='Run all KPIs once (test mode)')
    parser.add_argument('--frequency', type=str, help='Run specific frequency only (e.g., "1 min", "5 min", "15 min", "Daily")')

    args = parser.parse_args()

    if args.test:
        run_all_now()
    elif args.frequency:
        run_kpis_by_frequency(args.frequency)
    else:
        start_scheduler()
