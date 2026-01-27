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

os.environ['SSL_CERT_FILE'] = certifi.where()

# ============================================================
# CONFIGURATION
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "kpi_monitoring"
}

# Daily KPIs run time (24-hour format)
DAILY_RUN_HOUR = 15  # 11 PM
DAILY_RUN_MINUTE = 00  # 30 minutes past the hour

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
    Format the Flag column value based on outcome type.
    - Flag outcome: "true" or "false"
    - Sec outcome: numeric seconds (e.g., "2.5")
    - MB outcome: numeric megabytes (e.g., "0.75")
    - % outcome: numeric percentage (e.g., "85.5")
    """
    value = result.get('value')
    flag = result.get('flag')

    if outcome_type == 'Flag':
        # For Flag outcomes: true = problem, false = no problem
        return 'true' if flag else 'false'
    elif outcome_type == 'Sec':
        # For Sec outcomes: store numeric seconds
        return str(value) if value is not None else '0'
    elif outcome_type == 'MB':
        # For MB outcomes: store numeric megabytes
        return str(value) if value is not None else '0'
    elif outcome_type == '%':
        # For % outcomes: store numeric percentage (without % sign)
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
    """
    Determine if result is a hit or miss based on target comparison.
    """
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
            FROM kpi_results_history
            WHERE AssetId = %s AND KpiId = %s
            ORDER BY createdAt DESC
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
            SELECT Id FROM incidents
            WHERE AssetId = %s AND KpiId = %s AND Status = 'Open' AND Type = 'auto'
        """, (asset_id, kpi_id))

        incidents = cursor.fetchall()
        closed_count = 0

        for incident in incidents:
            cursor.execute("""
                UPDATE incidents
                SET Status = 'Closed', updatedAt = NOW()
                WHERE Id = %s
            """, (incident['Id'],))
            closed_count += 1
            print(f"      [AUTO-CLOSE] Incident #{incident['Id']} closed")

        return closed_count
    except Exception as e:
        print(f"      [ERROR] Auto-closing incident: {str(e)}")
        return 0

def store_in_results_history(cursor, asset_id, kpi_id, target, result_value, details):
    """Store KPI result in history table"""
    try:
        cursor.execute("""
            INSERT INTO kpi_results_history (AssetId, KpiId, Target, Result, Details)
            VALUES (%s, %s, %s, %s, %s)
        """, (asset_id, kpi_id, target, result_value, details))
        return cursor.lastrowid
    except Exception as e:
        print(f"      [ERROR] Storing in history: {str(e)}")
        return None

def check_consecutive_failures(cursor, asset_id, kpi_id, required_frequency):
    """Check if KPI has failed consecutively for the required number of times"""
    try:
        cursor.execute("""
            SELECT Target
            FROM kpi_results_history
            WHERE AssetId = %s AND KpiId = %s
            ORDER BY createdAt DESC
            LIMIT %s
        """, (asset_id, kpi_id, required_frequency))

        recent_results = cursor.fetchall()

        if len(recent_results) < required_frequency:
            return False

        return all(record['Target'] == 'miss' for record in recent_results)
    except Exception as e:
        print(f"      [ERROR] Checking consecutive failures: {str(e)}")
        return False

def create_incident(cursor, asset, kpi_id, kpi_name, severity_id):
    """Create an incident when a KPI check fails"""
    try:
        asset_id = asset['Id']

        cursor.execute("""
            SELECT Id FROM incidents
            WHERE AssetId = %s AND KpiId = %s AND Status = 'Open'
            LIMIT 1
        """, (asset_id, kpi_id))

        existing_incident = cursor.fetchone()

        if existing_incident:
            return existing_incident['Id'], False

        incident_title = f"{kpi_name} - Breach"
        description = f"{kpi_name} - Auto Created Incident"

        cursor.execute("""
            INSERT INTO incidents (MinistryId, DepartmentId, AssetId, KpiId,
                                   IncidentTitle, Description, SeverityId, Status, Type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'Open', 'auto')
        """, (asset.get('MinistryId'), asset.get('DepartmentId'),
              asset_id, kpi_id, incident_title, description, severity_id))

        return cursor.lastrowid, True
    except Exception as e:
        print(f"      [ERROR] Creating incident: {str(e)}")
        return None, False

def store_result(cursor, asset_id, kpi_id, result, outcome_type, target_value=None):
    """Store KPI result in the database using UPSERT logic"""
    try:
        # Format flag value based on outcome type (true/false, seconds, MB, %)
        flag_value = format_flag_value(result, outcome_type)
        result_value = format_result_value(result, outcome_type)
        details = result.get('details', '')

        target = determine_target_hit_miss(
            result.get('value'),
            target_value,
            outcome_type,
            result.get('flag')
        )

        cursor.execute("""
            INSERT INTO kpis_results (AssetId, KpiId, Flag, Target, Result, Details, createdAt)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                Flag = VALUES(Flag),
                Target = VALUES(Target),
                Result = VALUES(Result),
                Details = VALUES(Details),
                updatedAt = NOW()
        """, (asset_id, kpi_id, flag_value, target, result_value, details))

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

        # Determine target based on CitizenImpactLevel
        citizen_impact_level = asset.get('CitizenImpactLevel', 'Medium')
        if citizen_impact_level == 'High':
            target_value = kpi.get('TargetHigh')
        elif citizen_impact_level == 'Low':
            target_value = kpi.get('TargetLow')
        else:
            target_value = kpi.get('TargetMedium')

        # Store result
        store_result(cursor, asset['Id'], kpi['Id'], result, outcome_type, target_value)

        # Determine hit/miss
        target = determine_target_hit_miss(
            result.get('value'),
            target_value,
            outcome_type,
            result.get('flag')
        )

        # Store in history
        result_value = format_result_value(result, outcome_type)
        store_in_results_history(cursor, asset['Id'], kpi['Id'], target, result_value, result.get('details', ''))

        # Handle incidents using global incidentCreationFrequency
        if target == "miss":
            should_create = check_consecutive_failures(cursor, asset['Id'], kpi['Id'], incident_frequency)

            if should_create:
                severity_id = kpi.get('SeverityId')
                incident_id, is_new = create_incident(cursor, asset, kpi['Id'], kpi_name, severity_id)
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
        return None

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
            FROM assets a
            LEFT JOIN ministries m ON a.MinistryId = m.Id
            LEFT JOIN departments d ON a.DepartmentId = d.Id
            LEFT JOIN CommonLookup cl ON a.CitizenImpactLevelId = cl.Id
            WHERE a.deleted_at IS NULL
        """)
        assets = cursor.fetchall()

        # Get KPIs matching the frequency (include SeverityId for incident creation)
        cursor.execute("""
            SELECT Id, KpiName, KpiGroup, KpiType, Outcome,
                   TargetHigh, TargetMedium, TargetLow, Frequency, SeverityId
            FROM KpisLov
            WHERE KpiType IS NOT NULL AND Frequency = %s
        """, (frequency_filter,))
        kpis = cursor.fetchall()

        if not kpis:
            print(f"  No KPIs found with frequency: {frequency_filter}")
            return

        print(f"  Assets: {len(assets)} | KPIs: {len(kpis)}")

        total_checks = 0
        total_hits = 0
        total_misses = 0

        for asset in assets:
            print(f"\n  Asset: {asset['AssetName']} ({asset['CitizenImpactLevel'] or 'Medium'})")

            for kpi in kpis:
                total_checks += 1
                symbol = "[MISS]"
                result = run_kpi_for_asset(cursor, asset, kpi, incident_frequency)

                if result == "hit":
                    total_hits += 1
                    symbol = "[HIT]"
                elif result == "miss":
                    total_misses += 1
                    symbol = "[MISS]"
                else:
                    symbol = "[ERR]"

                print(f"    {symbol} {kpi['KpiName']}")

            conn.commit()

        print(f"\n  Summary: {total_checks} checks | {total_hits} hits | {total_misses} misses")

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
