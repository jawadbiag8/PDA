"""
Manual KPI Trigger API Server

Exposes an endpoint for manually triggering a specific KPI check for a specific asset.
Runs independently alongside the scheduler. Does not affect automated scheduling.

Usage:
    python src/api/server.py
"""

import sys
import os
import threading

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv

load_dotenv()

# Import shared logic from scheduler
from src.scheduler.scheduler_v3 import (
    get_db_connection,
    run_kpi_for_asset,
    run_browser_kpi_with_page,
    recalculate_asset_metrics,
    store_result,
    store_in_results_history,
    format_result_value,
    log,
)
from src.kpi_runners.browser_runner import SharedBrowserContext

# ============================================================
# CONFIGURATION
# ============================================================

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Estimated completion times by KPI type (for user-facing message)
ESTIMATED_TIMES = {
    'http': '5-10 seconds',
    'dns': '5-10 seconds',
    'ssl': '10-15 seconds',
    'browser': '15-20 seconds',
    'accessibility': '20-30 seconds',
}

# ============================================================
# FASTAPI APP
# ============================================================

app = FastAPI(title="KPI Manual Check API")


class ManualCheckRequest(BaseModel):
    kpiId: int
    assetId: int


@app.post("/api/kpi/manual-check")
def manual_kpi_check(request: ManualCheckRequest):
    """Trigger a manual KPI check for a specific asset. Returns immediately."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # Validate KPI exists
        cursor.execute("""
            SELECT Id, KpiName, KpiGroup, KpiType, Outcome,
                   TargetHigh, TargetMedium, TargetLow, Frequency, SeverityId
            FROM KpisLov
            WHERE Id = %s AND DeletedAt IS NULL
        """, (request.kpiId,))
        kpi = cursor.fetchone()

        if not kpi:
            return {"success": False, "message": f"KPI with ID {request.kpiId} not found"}

        if not kpi['KpiType']:
            return {"success": False, "message": f"KPI '{kpi['KpiName']}' has no runner type configured"}

        # Validate asset exists
        cursor.execute("""
            SELECT a.*, cl.Name as CitizenImpactLevel
            FROM Assets a
            LEFT JOIN CommonLookup cl ON a.CitizenImpactLevelId = cl.Id
            WHERE a.Id = %s AND a.DeletedAt IS NULL
        """, (request.assetId,))
        asset = cursor.fetchone()

        if not asset:
            return {"success": False, "message": f"Asset with ID {request.assetId} not found"}

        estimated_time = ESTIMATED_TIMES.get(kpi['KpiType'], '10-20 seconds')

        # Spawn background thread for KPI execution
        thread = threading.Thread(
            target=_run_manual_kpi_check,
            args=(asset, kpi),
            daemon=True
        )
        thread.start()

        return {
            "success": True,
            "message": (
                f"\"{kpi['KpiName']}\" manual check triggered for {asset['AssetName']}. "
                f"It will take approximately {estimated_time} to complete. "
                f"Please check the results shortly."
            ),
            "data": {
                "kpiId": kpi['Id'],
                "kpiName": kpi['KpiName'],
                "assetId": asset['Id'],
                "assetName": asset['AssetName'],
                "estimatedTime": estimated_time,
            }
        }

    except Exception as e:
        return {"success": False, "message": f"Error: {str(e)}"}
    finally:
        cursor.close()
        conn.close()


def _run_manual_kpi_check(asset, kpi):
    """Background worker: run a single KPI check with full logic (results, incidents, metrics)."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        log(f"[MANUAL] Triggered: {kpi['KpiName']} for {asset['AssetName']} (Asset {asset['Id']}) | URL: {asset['AssetUrl']}")

        # Get incident creation frequency
        cursor.execute("""
            SELECT Name FROM CommonLookup WHERE Type = 'IncidentCreationFrequency' LIMIT 1
        """)
        freq_row = cursor.fetchone()
        incident_frequency = int(freq_row['Name']) if freq_row else 3

        kpi_type = kpi['KpiType']

        if kpi_type in ('browser', 'accessibility'):
            # Browser-based KPIs need SharedBrowserContext
            try:
                with SharedBrowserContext() as ctx:
                    page, load_time, nav_success = ctx.navigate_to(asset['AssetUrl'])
                    if not nav_success:
                        log(f"[MANUAL] [WARN] Page load was slow/partial, running check anyway")
                    result = run_browser_kpi_with_page(cursor, asset, kpi, incident_frequency, page, load_time)
            except Exception as e:
                log(f"[MANUAL] [ERROR] Browser context failed: {str(e)}", "error")
                error_result = {'flag': True, 'value': None, 'details': f'Browser error: {str(e)[:200]}'}
                result_id = store_result(cursor, asset['Id'], kpi['Id'], error_result, kpi['Outcome'], target_override="skipped")
                result_value = format_result_value(error_result, kpi['Outcome'])
                store_in_results_history(cursor, asset['Id'], result_id, kpi['Id'], "skipped", result_value, f'Browser error: {str(e)[:200]}')
                result = "skipped"
        else:
            # Non-browser KPIs (http, dns, ssl)
            result = run_kpi_for_asset(cursor, asset, kpi, incident_frequency)

        conn.commit()

        # Recalculate metrics for this asset
        recalculate_asset_metrics(cursor, asset['Id'], asset.get('CitizenImpactLevel'))
        conn.commit()

        log(f"[MANUAL] Completed: {kpi['KpiName']} for {asset['AssetName']} | URL: {asset['AssetUrl']} | Result: {result}")

    except Exception as e:
        log(f"[MANUAL] [ERROR] {kpi['KpiName']} for {asset['AssetName']}: {str(e)}", "error")
        try:
            conn.commit()
        except:
            pass
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    log(f"Starting Manual KPI Check API on {API_HOST}:{API_PORT}")
    uvicorn.run(app, host=API_HOST, port=API_PORT)
