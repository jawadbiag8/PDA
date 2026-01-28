"""Test: Run scheduler logic for a single asset to validate DB writes"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()

from src.scheduler.scheduler_v3 import (
    get_db_connection, run_kpi_for_asset, store_result,
    store_in_results_history, format_result_value, recalculate_asset_metrics, DB_CONFIG
)

ASSET_ID = 1  # mowr.gov.pk

conn = get_db_connection()
cursor = conn.cursor(dictionary=True)

# Get incident frequency
cursor.execute("SELECT Name FROM CommonLookup WHERE Type = 'IncidentCreationFrequency' LIMIT 1")
freq_row = cursor.fetchone()
incident_frequency = int(freq_row['Name']) if freq_row else 3
print(f"Incident frequency: {incident_frequency}")

# Get the asset
cursor.execute("""
    SELECT a.*, m.MinistryName, d.DepartmentName, cl.Name as CitizenImpactLevel
    FROM Assets a
    LEFT JOIN Ministries m ON a.MinistryId = m.Id
    LEFT JOIN Departments d ON a.DepartmentId = d.Id
    LEFT JOIN CommonLookup cl ON a.CitizenImpactLevelId = cl.Id
    WHERE a.Id = %s
""", (ASSET_ID,))
asset = cursor.fetchone()
print(f"Asset: {asset['AssetName']} ({asset['CitizenImpactLevel']})")

# Get all automated KPIs (all frequencies)
cursor.execute("""
    SELECT Id, KpiName, KpiGroup, KpiType, Outcome,
           TargetHigh, TargetMedium, TargetLow, Frequency, SeverityId
    FROM KpisLov
    WHERE KpiType IS NOT NULL AND `Manual` = 'Auto' AND DeletedAt IS NULL
    ORDER BY Id
""")
kpis = cursor.fetchall()
print(f"KPIs to run: {len(kpis)}")
print("=" * 80)

site_is_down = False
total_checks = 0
total_hits = 0
total_misses = 0
total_skipped = 0

for kpi in kpis:
    kpi_name_lower = kpi['KpiName'].lower()

    if site_is_down:
        total_skipped += 1
        skipped_result = {'flag': True, 'value': None, 'details': 'Skipped - site is down'}
        result_id = store_result(cursor, asset['Id'], kpi['Id'], skipped_result, kpi['Outcome'], target_override="skipped")
        result_value = format_result_value(skipped_result, kpi['Outcome'])
        store_in_results_history(cursor, asset['Id'], result_id, kpi['Id'], "skipped", result_value, 'Skipped - site is down')
        print(f"  [SKIP] {kpi['KpiName']} (site is down)")
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

    print(f"  {symbol} {kpi['KpiName']}")

    if 'completely down' in kpi_name_lower and result == "miss":
        site_is_down = True
        print(f"  >> Site is DOWN - skipping remaining KPIs for this asset")

conn.commit()

# Recalculate metrics
recalculate_asset_metrics(cursor, asset['Id'], asset.get('CitizenImpactLevel'))
conn.commit()

print(f"\n{'='*80}")
print(f"Summary: {total_checks} checks | {total_hits} hits | {total_misses} misses | {total_skipped} skipped")
print(f"{'='*80}")

# Verify DB writes
print(f"\n--- Verifying DB writes for Asset {ASSET_ID} ---")

cursor.execute("SELECT COUNT(*) as cnt FROM kpisResults WHERE AssetId = %s", (ASSET_ID,))
print(f"kpisResults rows: {cursor.fetchone()['cnt']}")

cursor.execute("SELECT COUNT(*) as cnt FROM KPIsResultHistories WHERE AssetId = %s", (ASSET_ID,))
print(f"KPIsResultHistories rows: {cursor.fetchone()['cnt']}")

cursor.execute("SELECT COUNT(*) as cnt FROM Incidents WHERE AssetId = %s", (ASSET_ID,))
print(f"Incidents rows: {cursor.fetchone()['cnt']}")

cursor.execute("SELECT COUNT(*) as cnt FROM IncidentHistories WHERE AssetId = %s", (ASSET_ID,))
print(f"IncidentHistories rows: {cursor.fetchone()['cnt']}")

# Show target distribution
cursor.execute("SELECT Target, COUNT(*) as cnt FROM kpisResults WHERE AssetId = %s GROUP BY Target", (ASSET_ID,))
print(f"\nkpisResults Target distribution:")
for row in cursor.fetchall():
    print(f"  {row['Target']}: {row['cnt']}")

cursor.execute("SELECT Target, COUNT(*) as cnt FROM KPIsResultHistories WHERE AssetId = %s GROUP BY Target", (ASSET_ID,))
print(f"\nKPIsResultHistories Target distribution:")
for row in cursor.fetchall():
    print(f"  {row['Target']}: {row['cnt']}")

# Show AssetMetrics
cursor.execute("SELECT * FROM AssetMetrics WHERE AssetId = %s", (ASSET_ID,))
metrics = cursor.fetchone()
if metrics:
    print(f"\n--- AssetMetrics for Asset {ASSET_ID} ---")
    print(f"  Accessibility Index:    {metrics['AccessibilityIndex']}%")
    print(f"  Availability Index:     {metrics['AvailabilityIndex']}%")
    print(f"  Navigation Index:       {metrics['NavigationIndex']}%")
    print(f"  Performance Index:      {metrics['PerformanceIndex']}%")
    print(f"  Security Index:         {metrics['SecurityIndex']}%")
    print(f"  User Experience Index:  {metrics['UserExperienceIndex']}%")
    print(f"  ---")
    print(f"  Citizen Happiness (CHM):  {metrics['CitizenHappinessMetric']}%")
    print(f"  Overall Compliance (OCM): {metrics['OverallComplianceMetric']}%")
    print(f"  Risk Exposure (DREI):     {metrics['DigitalRiskExposureIndex']}%")
    print(f"  Current Health:           {metrics['CurrentHealth']}%")
    print(f"  ---")
    print(f"  Period: {metrics['PeriodStartDate']} to {metrics['PeriodEndDate']}")
    print(f"  Calculated at: {metrics['CalculatedAt']}")
else:
    print(f"\n  No AssetMetrics row found for Asset {ASSET_ID}")

cursor.close()
conn.close()
