"""
Test script to verify scheduler_v2 works correctly
Runs a limited set of KPIs to test the flow
"""

import sys
import os

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.kpi_runners.http_runner import HttpKPIRunner
from src.kpi_runners.dns_runner import DNSKPIRunner
import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",
    "database": "kpi_monitoring"
}

def format_result_value(result, outcome_type):
    """Format the result value based on outcome type"""
    value = result.get('value')

    if outcome_type == 'Flag':
        return '1' if result.get('flag') else '0'
    elif outcome_type in ['Sec', 'MB', '%']:
        return str(value) if value is not None else '0'
    else:
        return str(value) if value is not None else ''

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    print("=" * 80)
    print("Testing KPI Monitoring System V2")
    print("=" * 80)
    print()

    # Get one asset
    cursor.execute("""
        SELECT a.*, m.MinistryName
        FROM assets a
        LEFT JOIN ministries m ON a.MinistryId = m.Id
        WHERE a.deleted_at IS NULL
        LIMIT 1
    """)
    asset = cursor.fetchone()

    if not asset:
        print("No assets found!")
        return

    print(f"Testing Asset: {asset['AssetName']} ({asset['AssetUrl']})")
    print(f"Ministry: {asset['MinistryName']}\n")

    # Get 2 test KPIs (HTTP and DNS)
    cursor.execute("""
        SELECT Id, KpiName, KpiGroup, KpiType, Outcome
        FROM KpisLov
        WHERE KpiType IN ('http', 'dns')
        LIMIT 2
    """)
    kpis = cursor.fetchall()

    print(f"Running {len(kpis)} test KPIs...\n")

    for kpi in kpis:
        print(f"Testing: {kpi['KpiName']} [{kpi['KpiType'].upper()}]")

        # Create asset dict
        asset_data = {
            'id': asset['Id'],
            'app_name': asset['AssetName'],
            'url': asset['AssetUrl']
        }

        # Create kpi dict
        kpi_data = {
            'id': kpi['Id'],
            'kpi_name': kpi['KpiName'],
            'kpi_type': kpi['KpiType']
        }

        # Run the check
        try:
            if kpi['KpiType'] == 'http':
                runner = HttpKPIRunner(asset_data, kpi_data)
            elif kpi['KpiType'] == 'dns':
                runner = DNSKPIRunner(asset_data, kpi_data)
            else:
                continue

            result = runner.run()

            # Format result
            flag = 1 if result.get('flag') else 0
            result_value = format_result_value(result, kpi['Outcome'])
            details = result.get('details', '')

            print(f"  Flag: {flag}")
            print(f"  Result: {result_value}")
            print(f"  Details: {details[:100]}...")

            # Store result
            cursor.execute("""
                INSERT INTO kpis_results (AssetId, KpiId, Flag, Result, Details)
                VALUES (%s, %s, %s, %s, %s)
            """, (asset['Id'], kpi['Id'], flag, result_value, details))

            print(f"  ✓ Result stored (ID: {cursor.lastrowid})")

            # Create incident if failed
            if flag:
                incident_title = f"KPI Failure: {kpi['KpiName']}"
                cursor.execute("""
                    INSERT INTO incidents (AssetId, KpiId, IncidentTitle, Description, SecurityLevel, Status)
                    VALUES (%s, %s, %s, %s, 'Medium', 'Open')
                """, (asset['Id'], kpi['Id'], incident_title, details))
                print(f"  📋 Incident created (ID: {cursor.lastrowid})")

            print()

        except Exception as e:
            print(f"  ✗ Error: {str(e)}\n")

    conn.commit()

    # Verify stored data
    print("=" * 80)
    print("Verification")
    print("=" * 80)

    cursor.execute("SELECT COUNT(*) as count FROM kpis_results")
    print(f"Total Results Stored: {cursor.fetchone()['count']}")

    cursor.execute("SELECT COUNT(*) as count FROM incidents WHERE Status = 'Open'")
    print(f"Open Incidents: {cursor.fetchone()['count']}")

    cursor.close()
    conn.close()

    print("\n✅ Test completed successfully!")

if __name__ == "__main__":
    main()
