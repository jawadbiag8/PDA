"""
Frequency-Based KPI Scheduler v3
Runs KPIs at their defined intervals based on the Frequency column in KpisLov table.

Frequencies supported:
- "1 min" - runs every 1 minute
- "5 min" - runs every 5 minutes
- "15 min" - runs every 15 minutes
- "Daily" - runs once daily at DAILY_RUN_TIME (configurable)

Usage:
- python scheduler_v3.py --start     # Start scheduler as background process
- python scheduler_v3.py --stop      # Stop running scheduler
- python scheduler_v3.py --test      # Run all KPIs once (test mode)
- python scheduler_v3.py --frequency "1 min"  # Run specific frequency only
"""

import sys
import os
import logging
import signal
import subprocess

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

# Logging configuration
LOG_PATH = os.getenv("LOG_PATH", "/var/logs/kpiAutomationLogs/")
PID_FILE = os.path.join(LOG_PATH, "scheduler.pid")

# Daily KPIs run time (24-hour format) - configurable via .env
DAILY_RUN_HOUR = int(os.getenv("DAILY_RUN_HOUR", "15"))  # Default: 3 PM
DAILY_RUN_MINUTE = int(os.getenv("DAILY_RUN_MINUTE", "0"))  # Default: Start of hour

# ============================================================
# LOGGING SETUP
# ============================================================

def setup_logging():
    """Setup date-wise logging to file"""
    # Create log directory if it doesn't exist
    os.makedirs(LOG_PATH, exist_ok=True)

    # Date-wise log file name
    log_filename = datetime.now().strftime("%Y-%m-%d") + ".log"
    log_file = os.path.join(LOG_PATH, log_filename)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

    return logging.getLogger(__name__)

# Initialize logger
logger = setup_logging()

def log(message, level="info"):
    """Log message to both file and console"""
    # Check if date changed, rotate log file if needed
    current_date = datetime.now().strftime("%Y-%m-%d")
    expected_log_file = os.path.join(LOG_PATH, f"{current_date}.log")

    # Check if we need to rotate to a new date file
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            if handler.baseFilename != expected_log_file:
                # Date changed, update file handler
                handler.close()
                logger.removeHandler(handler)
                new_handler = logging.FileHandler(expected_log_file, mode='a', encoding='utf-8')
                new_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
                logger.addHandler(new_handler)
            break

    if level == "error":
        logger.error(message)
    elif level == "warning":
        logger.warning(message)
    else:
        logger.info(message)

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
        log(f"[ERROR] Checking consecutive hits: {str(e)}", "error")
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
            log(f"[AUTO-CLOSE] Incident #{incident['Id']} resolved")

        return closed_count
    except Exception as e:
        log(f"[ERROR] Auto-closing incident: {str(e)}", "error")
        return 0

def store_in_results_history(cursor, asset_id, kpis_result_id, kpi_id, target, result_value, details):
    """Store KPI result in history table (KPIsResultHistories)"""
    try:
        if not kpis_result_id:
            log(f"[WARN] No kpisResults ID, skipping history insert", "warning")
            return None

        cursor.execute("""
            INSERT INTO KPIsResultHistories (AssetId, KPIsResultId, KpiId, Details, CreatedAt, Target, Result)
            VALUES (%s, %s, %s, %s, NOW(), %s, %s)
        """, (asset_id, kpis_result_id, kpi_id, details, target, result_value))
        return cursor.lastrowid
    except Exception as e:
        log(f"[ERROR] Storing in history: {str(e)}", "error")
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
        log(f"[ERROR] Checking consecutive failures: {str(e)}", "error")
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
        log(f"[ERROR] Creating incident: {str(e)}", "error")
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
        log(f"[ERROR] Storing result: {str(e)}", "error")
        return None

# ============================================================
# ASSET METRICS CALCULATION
# ============================================================

def recalculate_asset_metrics(cursor, asset_id, citizen_impact_level):
    """
    Recalculate all metrics for a single asset based on last 30 days of data.
    Called after each asset's KPI cycle completes.
    """
    try:
        from datetime import timedelta
        period_end = datetime.now()
        period_start = period_end - timedelta(days=30)

        # ----------------------------------------------------------
        # 1. Load weight configuration from MetricWeights
        # ----------------------------------------------------------
        chm_weights = {}
        cursor.execute("SELECT Name, Weight FROM MetricWeights WHERE Category = 'CHM'")
        for row in cursor.fetchall():
            chm_weights[row['Name']] = float(row['Weight'])

        ocm_weights = {}
        cursor.execute("SELECT Name, Weight FROM MetricWeights WHERE Category = 'OCM'")
        for row in cursor.fetchall():
            ocm_weights[row['Name']] = float(row['Weight'])

        drei_weights = {}
        cursor.execute("SELECT Name, Weight FROM MetricWeights WHERE Category = 'DREI'")
        for row in cursor.fetchall():
            drei_weights[row['Name']] = float(row['Weight'])

        criticality_map = {}
        cursor.execute("SELECT Name, Weight FROM MetricWeights WHERE Category = 'AssetCriticality'")
        for row in cursor.fetchall():
            criticality_map[row['Name'].upper()] = float(row['Weight'])

        # ----------------------------------------------------------
        # 2. Calculate KPI Group Indexes (hit rate per KPI, weighted)
        # ----------------------------------------------------------
        # Get all automated KPIs with their group and weight
        cursor.execute("""
            SELECT Id, KpiName, KpiGroup, Weight
            FROM KpisLov
            WHERE `Manual` = 'Auto' AND DeletedAt IS NULL AND KpiType IS NOT NULL
        """)
        all_kpis = cursor.fetchall()

        # Get last 30 days of results for this asset
        cursor.execute("""
            SELECT KpiId, Target
            FROM KPIsResultHistories
            WHERE AssetId = %s AND CreatedAt >= %s AND Target IN ('hit', 'miss')
        """, (asset_id, period_start))
        history_rows = cursor.fetchall()

        # Calculate hit rate per KPI
        kpi_stats = {}  # {kpi_id: {'hits': 0, 'total': 0}}
        for row in history_rows:
            kpi_id = row['KpiId']
            if kpi_id not in kpi_stats:
                kpi_stats[kpi_id] = {'hits': 0, 'total': 0}
            kpi_stats[kpi_id]['total'] += 1
            if row['Target'] == 'hit':
                kpi_stats[kpi_id]['hits'] += 1

        # Group KPIs and calculate weighted index per group
        group_indexes = {}
        for kpi in all_kpis:
            group = kpi['KpiGroup']
            weight = kpi['Weight'] or 0
            kpi_id = kpi['Id']

            if group not in group_indexes:
                group_indexes[group] = {'weighted_sum': 0, 'total_weight': 0}

            if kpi_id in kpi_stats and kpi_stats[kpi_id]['total'] > 0:
                hit_rate = (kpi_stats[kpi_id]['hits'] / kpi_stats[kpi_id]['total']) * 100
            else:
                hit_rate = 0  # No data = 0

            group_indexes[group]['weighted_sum'] += hit_rate * weight
            group_indexes[group]['total_weight'] += weight

        # Calculate final index per group (0-100)
        group_scores = {}
        for group, data in group_indexes.items():
            if data['total_weight'] > 0:
                group_scores[group] = data['weighted_sum'] / data['total_weight']
            else:
                group_scores[group] = 0

        accessibility_idx = group_scores.get('Accessibility & Inclusivity', 0)
        availability_idx = group_scores.get('Availability & Reliability', 0)
        navigation_idx = group_scores.get('Navigation & Discoverability', 0)
        performance_idx = group_scores.get('Performance & Efficiency', 0)
        security_idx = group_scores.get('Security, Trust & Privacy', 0)
        ux_idx = group_scores.get('User Experience & Journey Quality', 0)

        # ----------------------------------------------------------
        # 3. Calculate CHM (Citizen Happiness Metric)
        # ----------------------------------------------------------
        chm_total_weight = sum(chm_weights.values())
        chm = 0
        if chm_total_weight > 0:
            for group, weight in chm_weights.items():
                chm += group_scores.get(group, 0) * weight
            chm /= chm_total_weight

        # ----------------------------------------------------------
        # 4. Calculate OCM (Overall Compliance Metric)
        # ----------------------------------------------------------
        ocm_total_weight = sum(ocm_weights.values())
        ocm = 0
        if ocm_total_weight > 0:
            for group, weight in ocm_weights.items():
                ocm += group_scores.get(group, 0) * weight
            ocm /= ocm_total_weight

        # ----------------------------------------------------------
        # 5. Calculate DREI (Digital Risk Exposure Index)
        # ----------------------------------------------------------
        # Get severity names for mapping
        severity_map = {}  # {severity_id: severity_name}
        cursor.execute("SELECT Id, Name FROM CommonLookup WHERE Type = 'SeverityLevel'")
        for row in cursor.fetchall():
            severity_map[row['Id']] = row['Name']

        # Count incidents by severity (all time for this asset)
        cursor.execute("""
            SELECT SeverityId, Status, COUNT(*) as cnt
            FROM Incidents
            WHERE AssetId = %s AND DeletedAt IS NULL
            GROUP BY SeverityId, Status
        """, (asset_id,))

        incident_counts = {}  # {'P1': {'open': 0, 'total': 0}, ...}
        for row in cursor.fetchall():
            sev_name = severity_map.get(row['SeverityId'], '')
            if sev_name not in incident_counts:
                incident_counts[sev_name] = {'open': 0, 'total': 0}
            incident_counts[sev_name]['total'] += row['cnt']
            if row['Status'] == 'Open':
                incident_counts[sev_name]['open'] += row['cnt']

        # Map severity names to DREI categories
        # P1=Critical, P2=High, P3=Medium, P4=Low
        sev_to_drei = {'P1': 'OpenCritical', 'P2': 'OpenHigh', 'P3': 'OpenMedium', 'P4': 'OpenLow'}

        drei_component = 0
        for sev_name, drei_key in sev_to_drei.items():
            data = incident_counts.get(sev_name, {'open': 0, 'total': 0})
            ratio = (data['open'] / data['total'] * 100) if data['total'] > 0 else 0
            drei_component += ratio * drei_weights.get(drei_key, 0)

        # SLA Breach: % of misses in last 30 days
        total_checks = sum(s['total'] for s in kpi_stats.values())
        total_hits = sum(s['hits'] for s in kpi_stats.values())
        total_misses = total_checks - total_hits
        sla_breach_pct = (total_misses / total_checks * 100) if total_checks > 0 else 0
        drei_component += sla_breach_pct * drei_weights.get('SLABreach', 0)

        drei_total_weight = sum(drei_weights.values())
        raw_drei = drei_component / drei_total_weight if drei_total_weight > 0 else 0

        # Apply asset criticality
        level = (citizen_impact_level or '').upper()
        asset_criticality_pct = 30  # default Low
        for crit_level, crit_pct in criticality_map.items():
            if level.startswith(crit_level):
                asset_criticality_pct = crit_pct
                break

        drei = raw_drei * (asset_criticality_pct / 100)

        # ----------------------------------------------------------
        # 6. Calculate Current Health
        # ----------------------------------------------------------
        current_health = (ocm + (100 - drei)) / 2

        # ----------------------------------------------------------
        # 7. UPSERT into AssetMetrics
        # ----------------------------------------------------------
        cursor.execute("""
            INSERT INTO AssetMetrics (AssetId, AccessibilityIndex, AvailabilityIndex, NavigationIndex,
                                      PerformanceIndex, SecurityIndex, UserExperienceIndex,
                                      CitizenHappinessMetric, OverallComplianceMetric,
                                      DigitalRiskExposureIndex, CurrentHealth,
                                      PeriodStartDate, PeriodEndDate, CalculatedAt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                AccessibilityIndex = VALUES(AccessibilityIndex),
                AvailabilityIndex = VALUES(AvailabilityIndex),
                NavigationIndex = VALUES(NavigationIndex),
                PerformanceIndex = VALUES(PerformanceIndex),
                SecurityIndex = VALUES(SecurityIndex),
                UserExperienceIndex = VALUES(UserExperienceIndex),
                CitizenHappinessMetric = VALUES(CitizenHappinessMetric),
                OverallComplianceMetric = VALUES(OverallComplianceMetric),
                DigitalRiskExposureIndex = VALUES(DigitalRiskExposureIndex),
                CurrentHealth = VALUES(CurrentHealth),
                PeriodStartDate = VALUES(PeriodStartDate),
                PeriodEndDate = VALUES(PeriodEndDate),
                CalculatedAt = NOW()
        """, (asset_id, accessibility_idx, availability_idx, navigation_idx,
              performance_idx, security_idx, ux_idx,
              chm, ocm, drei, current_health,
              period_start, period_end))

        log(f"[METRICS] Asset {asset_id} | Health={current_health:.1f}% | CHM={chm:.1f}% | OCM={ocm:.1f}% | DREI={drei:.1f}%")

    except Exception as e:
        log(f"[ERROR] Recalculating metrics for Asset {asset_id}: {str(e)}", "error")

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
        log(f"[SKIP] Unknown KPI type: {kpi_type}", "warning")
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
                    log(f"[INCIDENT] #{incident_id} created (after {incident_frequency} consecutive misses)")
                elif incident_id:
                    log(f"[EXISTING] Incident #{incident_id} already open")
            else:
                log(f"[WAIT] Need {incident_frequency} consecutive misses")
        else:
            # Auto-close only after consecutive hits
            should_close = check_consecutive_hits(cursor, asset['Id'], kpi['Id'], incident_frequency)
            if should_close:
                auto_close_incident(cursor, asset['Id'], kpi['Id'])

        return target

    except Exception as e:
        log(f"[ERROR] {str(e)}", "error")

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

    log("=" * 80)
    log(f"Running KPIs with frequency: {frequency_filter}")
    log("=" * 80)

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
            log(f"No KPIs found with frequency: {frequency_filter}", "warning")
            return

        log(f"Assets: {len(assets)} | KPIs: {len(kpis)}")

        total_checks = 0
        total_hits = 0
        total_misses = 0
        total_skipped = 0

        for asset in assets:
            log(f"Asset: {asset['AssetName']} ({asset['CitizenImpactLevel'] or 'N/A'}) | URL: {asset['AssetUrl']}")
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
                    log(f"  [SKIP] {kpi['KpiName']} (site is down)")
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

                log(f"  {symbol} {kpi['KpiName']}")

                # Check if this was the "site completely down" KPI and it missed
                if 'completely down' in kpi_name_lower and result == "miss":
                    site_is_down = True
                    log(f"  >> Site is DOWN - skipping remaining KPIs for this asset")

            conn.commit()

            # Recalculate metrics for this asset after all KPIs are done
            recalculate_asset_metrics(cursor, asset['Id'], asset.get('CitizenImpactLevel'))
            conn.commit()

        log(f"Summary: {total_checks} checks | {total_hits} hits | {total_misses} misses | {total_skipped} skipped")

    except Exception as e:
        log(f"[ERROR] {str(e)}", "error")
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
    log("=" * 80)
    log("RUNNING ALL KPIs (TEST MODE)")
    log("=" * 80)

    frequencies = ["1 min", "5 min", "15 min", "Daily"]
    for freq in frequencies:
        run_kpis_by_frequency(freq)

    log("=" * 80)
    log("ALL KPIs COMPLETED")
    log("=" * 80)

def write_pid_file():
    """Write current process PID to file"""
    os.makedirs(LOG_PATH, exist_ok=True)
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    log(f"PID file created: {PID_FILE} (PID: {os.getpid()})")

def remove_pid_file():
    """Remove PID file on shutdown"""
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)
        log("PID file removed")

def get_running_pid():
    """Get PID of running scheduler, or None if not running"""
    if not os.path.exists(PID_FILE):
        return None
    try:
        with open(PID_FILE, 'r') as f:
            pid = int(f.read().strip())
        # Check if process is actually running
        if sys.platform == 'win32':
            # Windows: use tasklist
            result = subprocess.run(['tasklist', '/FI', f'PID eq {pid}'], capture_output=True, text=True)
            if str(pid) in result.stdout:
                return pid
        else:
            # Unix: send signal 0 to check if process exists
            os.kill(pid, 0)
            return pid
    except (ValueError, ProcessLookupError, OSError):
        # PID file exists but process is not running
        remove_pid_file()
    return None

# Global scheduler instance for graceful shutdown
_scheduler = None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global _scheduler
    log(f"Received signal {signum}, shutting down...")

    if _scheduler:
        log("Waiting for current job to complete...")
        _scheduler.shutdown(wait=True)  # Wait for current job to finish
        log("Scheduler shutdown complete")

    remove_pid_file()
    sys.exit(0)

def start_scheduler():
    """Start the scheduler with all jobs"""
    # Check if already running
    existing_pid = get_running_pid()
    if existing_pid:
        log(f"Scheduler is already running (PID: {existing_pid})", "error")
        print(f"Scheduler is already running (PID: {existing_pid})")
        print(f"Use --stop to stop it first.")
        sys.exit(1)

    # Write PID file
    write_pid_file()

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    if sys.platform != 'win32':
        signal.signal(signal.SIGHUP, signal_handler)

    global _scheduler
    _scheduler = BlockingScheduler()

    log("=" * 80)
    log("KPI MONITORING SCHEDULER v3 - STARTED")
    log("=" * 80)
    log(f"Schedule Configuration:")
    log(f"  - 1 min KPIs: Every 1 minute")
    log(f"  - 5 min KPIs: Every 5 minutes")
    log(f"  - 15 min KPIs: Every 15 minutes")
    log(f"  - Daily KPIs: Every day at {DAILY_RUN_HOUR:02d}:{DAILY_RUN_MINUTE:02d}")
    log(f"Log Path: {LOG_PATH}")
    log(f"PID File: {PID_FILE}")

    # Schedule jobs with coalesce=True to merge missed runs, and misfire_grace_time
    # coalesce=True: If job was missed multiple times, run only once
    # misfire_grace_time: How many seconds late a job can be and still run
    _scheduler.add_job(job_1_minute, IntervalTrigger(minutes=1), id='kpi_1min', name='1-minute KPIs',
                       coalesce=True, max_instances=1, misfire_grace_time=60)
    _scheduler.add_job(job_5_minute, IntervalTrigger(minutes=5), id='kpi_5min', name='5-minute KPIs',
                       coalesce=True, max_instances=1, misfire_grace_time=120)
    _scheduler.add_job(job_15_minute, IntervalTrigger(minutes=15), id='kpi_15min', name='15-minute KPIs',
                       coalesce=True, max_instances=1, misfire_grace_time=180)
    _scheduler.add_job(job_daily, CronTrigger(hour=DAILY_RUN_HOUR, minute=DAILY_RUN_MINUTE), id='kpi_daily', name='Daily KPIs',
                       coalesce=True, max_instances=1, misfire_grace_time=300)

    # Run 1-minute KPIs immediately on startup
    log("Running initial checks...")
    job_1_minute()

    try:
        _scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log("Scheduler stopped.")
        remove_pid_file()

def stop_scheduler():
    """Stop the running scheduler"""
    pid = get_running_pid()
    if not pid:
        print("Scheduler is not running.")
        log("Stop requested but scheduler is not running", "warning")
        return

    log(f"Stopping scheduler (PID: {pid})...")
    print(f"Stopping scheduler (PID: {pid})...")

    try:
        if sys.platform == 'win32':
            # Windows: use taskkill
            subprocess.run(['taskkill', '/F', '/PID', str(pid)], check=True)
        else:
            # Unix: send SIGTERM
            os.kill(pid, signal.SIGTERM)

        # Wait a moment and verify it stopped
        import time
        time.sleep(2)

        if get_running_pid():
            log("Scheduler did not stop gracefully, forcing...", "warning")
            if sys.platform == 'win32':
                subprocess.run(['taskkill', '/F', '/PID', str(pid)], check=True)
            else:
                os.kill(pid, signal.SIGKILL)

        remove_pid_file()
        log("Scheduler stopped successfully")
        print("Scheduler stopped successfully.")

    except Exception as e:
        log(f"Error stopping scheduler: {str(e)}", "error")
        print(f"Error stopping scheduler: {str(e)}")

def start_daemon():
    """Start the scheduler as a background process"""
    # Check if already running
    existing_pid = get_running_pid()
    if existing_pid:
        print(f"Scheduler is already running (PID: {existing_pid})")
        print(f"Use --stop to stop it first.")
        return

    log("Starting scheduler as background process...")
    print("Starting scheduler as background process...")

    # Get the path to this script
    script_path = os.path.abspath(__file__)

    if sys.platform == 'win32':
        # Windows: use pythonw to run without console, or start /B
        # Using subprocess with CREATE_NO_WINDOW flag
        CREATE_NO_WINDOW = 0x08000000
        process = subprocess.Popen(
            [sys.executable, script_path, '--run-daemon'],
            creationflags=CREATE_NO_WINDOW,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL
        )
        print(f"Scheduler started in background (PID: {process.pid})")
        print(f"Logs are being written to: {LOG_PATH}")
    else:
        # Unix: fork and detach
        pid = os.fork()
        if pid > 0:
            # Parent process
            print(f"Scheduler started in background (PID: {pid})")
            print(f"Logs are being written to: {LOG_PATH}")
            sys.exit(0)
        else:
            # Child process - become daemon
            os.setsid()
            # Fork again to prevent zombie
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
            # Now we're the daemon
            start_scheduler()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='KPI Monitoring Scheduler v3')
    parser.add_argument('--start', action='store_true', help='Start scheduler as background process')
    parser.add_argument('--stop', action='store_true', help='Stop running scheduler')
    parser.add_argument('--test', action='store_true', help='Run all KPIs once (test mode)')
    parser.add_argument('--frequency', type=str, help='Run specific frequency only (e.g., "1 min", "5 min", "15 min", "Daily")')
    parser.add_argument('--run-daemon', action='store_true', help=argparse.SUPPRESS)  # Internal use

    args = parser.parse_args()

    if args.start:
        start_daemon()
    elif args.stop:
        stop_scheduler()
    elif args.test:
        run_all_now()
    elif args.frequency:
        run_kpis_by_frequency(args.frequency)
    elif args.run_daemon:
        # Internal: called when starting as daemon
        start_scheduler()
    else:
        # Default: run in foreground
        start_scheduler()
