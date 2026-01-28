"""Quick test: Run all KPIs against a single asset"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import mysql.connector
import certifi

os.environ['SSL_CERT_FILE'] = certifi.where()

from src.scheduler.scheduler_v3 import (
    get_runner, format_flag_value, format_result_value,
    determine_target_hit_miss, DB_CONFIG
)

ASSET_ID = 2  # ead.gov.pk

conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(dictionary=True)

# Get the asset
cursor.execute("""
    SELECT a.*, cl.Name as CitizenImpactLevel
    FROM Assets a
    LEFT JOIN CommonLookup cl ON a.CitizenImpactLevelId = cl.Id
    WHERE a.Id = %s
""", (ASSET_ID,))
asset = cursor.fetchone()
print(f"Asset: {asset['AssetName']} ({asset['AssetUrl']})")
print(f"CitizenImpactLevel: {asset['CitizenImpactLevel']}")
print("=" * 80)

# Get ALL automated KPIs
cursor.execute("""
    SELECT Id, KpiName, KpiGroup, KpiType, Outcome, SeverityId,
           TargetHigh, TargetMedium, TargetLow, Frequency
    FROM KpisLov
    WHERE KpiType IS NOT NULL AND `Manual` = 'Auto' AND DeletedAt IS NULL
    ORDER BY KpiGroup, Id
""")
kpis = cursor.fetchall()

asset_data = {'id': asset['Id'], 'app_name': asset['AssetName'], 'url': asset['AssetUrl']}

current_group = ''
total_hits = 0
total_misses = 0
total_errors = 0

for kpi in kpis:
    if kpi['KpiGroup'] != current_group:
        current_group = kpi['KpiGroup']
        print(f"\n--- {current_group} ---")

    kpi_data = {'id': kpi['Id'], 'kpi_name': kpi['KpiName'], 'kpi_type': kpi['KpiType']}
    runner = get_runner(kpi['KpiType'], asset_data, kpi_data)

    if not runner:
        print(f"  [SKIP] {kpi['KpiName']} (no runner for {kpi['KpiType']})")
        continue

    try:
        result = runner.run()
        outcome = kpi['Outcome']

        # Get target based on impact level (prefix matching for CommonLookup values)
        impact = (asset.get('CitizenImpactLevel') or '').upper()
        if impact.startswith('HIGH'):
            tv = kpi.get('TargetHigh')
        elif impact.startswith('LOW'):
            tv = kpi.get('TargetLow')
        else:
            tv = kpi.get('TargetMedium')

        flag_val = format_flag_value(result, outcome)
        result_val = format_result_value(result, outcome)
        target = determine_target_hit_miss(result.get('value'), tv, outcome, result.get('flag'))

        if target == 'hit':
            total_hits += 1
            symbol = 'HIT '
        else:
            total_misses += 1
            symbol = 'MISS'

        print(f"  [{symbol}] {kpi['KpiName']}")
        print(f"         Flag={flag_val} | Result={result_val} | Target={target} | Outcome={outcome}")
        details = result.get('details', '')
        if details:
            print(f"         Details: {details[:120]}")
    except Exception as e:
        total_errors += 1
        print(f"  [ERR]  {kpi['KpiName']}: {str(e)[:100]}")

print(f"\n{'='*80}")
print(f"Summary: {total_hits} hits | {total_misses} misses | {total_errors} errors")
print(f"{'='*80}")

cursor.close()
conn.close()
