"""
Manual KPI Trigger API Server

Exposes an endpoint for manually triggering a specific KPI check for a specific asset.
Runs independently alongside the scheduler. Does not affect automated scheduling.

Usage:
    python src/api/server.py
"""

import sys
import os
import logging
import threading
from datetime import datetime

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# LOGGING SETUP (separate from scheduler logs)
# ============================================================

MANUAL_LOG_PATH = os.getenv("MANUAL_LOG_PATH", "/var/logs/kpiMannual/")

def setup_manual_logging():
    """Setup date-wise logging for the manual API server"""
    os.makedirs(MANUAL_LOG_PATH, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d") + ".log"
    log_file = os.path.join(MANUAL_LOG_PATH, log_filename)

    logger = logging.getLogger("manual_kpi")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Don't propagate to root logger (prevents writing to auto log files)

    # Avoid adding duplicate handlers on reimport
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

_logger = setup_manual_logging()

def log(message, level="info"):
    """Log to manual API log files (date-wise rotation)"""
    # Check if date changed, rotate log file if needed
    current_date = datetime.now().strftime("%Y-%m-%d")
    expected_log_file = os.path.join(MANUAL_LOG_PATH, f"{current_date}.log")

    for handler in _logger.handlers:
        if isinstance(handler, logging.FileHandler):
            if handler.baseFilename != os.path.abspath(expected_log_file):
                handler.close()
                _logger.removeHandler(handler)
                new_handler = logging.FileHandler(expected_log_file, mode='a', encoding='utf-8')
                new_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))
                _logger.addHandler(new_handler)
            break

    if level == "error":
        _logger.error(message)
    elif level == "warning":
        _logger.warning(message)
    else:
        _logger.info(message)

# Import shared logic from scheduler (import AFTER defining log so we can override)
from src.scheduler.scheduler_v3 import (
    get_db_connection,
    run_kpi_for_asset,
    run_browser_kpi_with_page,
    recalculate_asset_metrics,
    store_result,
    store_in_results_history,
    format_result_value,
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
                f"It will take approximately {estimated_time} to complete. "
                f"Please check the results shortly."
            ),
            "data": {
                "kpiId": kpi['Id'],
                "kpiName": kpi['KpiName'],
                "assetId": asset['Id'],
                "assetName": asset['AssetName'],
                "assetUrl": asset['AssetUrl'],
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
    conn = None
    cursor = None

    try:
        log(f"[MANUAL] Triggered: {kpi['KpiName']} for {asset['AssetName']} (Asset {asset['Id']}) | URL: {asset['AssetUrl']}")

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

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
        import traceback
        log(f"[MANUAL] [TRACEBACK] {traceback.format_exc()}", "error")
        try:
            if conn:
                conn.commit()
        except:
            pass
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


API_PID_FILE = os.path.join(MANUAL_LOG_PATH, "api_server.pid")


def _find_pid_by_port():
    """Find PID of the process listening on API_PORT (fallback when PID file is missing)"""
    import subprocess
    try:
        result = subprocess.run(
            ['lsof', '-ti', f':{API_PORT}'],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.strip().split('\n')[0])
    except Exception:
        pass
    return None


def start_detached():
    """Start the API server as a detached background process"""
    os.makedirs(MANUAL_LOG_PATH, exist_ok=True)

    # Check if already running via PID file
    if os.path.exists(API_PID_FILE):
        try:
            with open(API_PID_FILE, 'r') as f:
                pid = int(f.read().strip())
            if sys.platform == 'win32':
                import subprocess
                result = subprocess.run(['tasklist', '/FI', f'PID eq {pid}'], capture_output=True, text=True)
                if str(pid) in result.stdout:
                    print(f"API server is already running (PID: {pid})")
                    return
            else:
                os.kill(pid, 0)
                print(f"API server is already running (PID: {pid})")
                return
        except (ValueError, ProcessLookupError, OSError):
            os.remove(API_PID_FILE)

    # Also check if something is already listening on the port
    existing_pid = _find_pid_by_port()
    if existing_pid:
        print(f"Port {API_PORT} is already in use (PID: {existing_pid}). Stop it first.")
        return

    script_path = os.path.abspath(__file__)

    if sys.platform == 'win32':
        import subprocess
        CREATE_NO_WINDOW = 0x08000000
        process = subprocess.Popen(
            [sys.executable, script_path, '--run-server'],
            creationflags=CREATE_NO_WINDOW,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL
        )
        print(f"API server started in background (PID: {process.pid})")
        print(f"Listening on: http://{API_HOST}:{API_PORT}")
        print(f"Logs: {MANUAL_LOG_PATH}")
    else:
        # Unix: use subprocess instead of double-fork for reliable PID tracking
        import subprocess
        process = subprocess.Popen(
            [sys.executable, script_path, '--run-server'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            start_new_session=True
        )
        print(f"API server started in background (PID: {process.pid})")
        print(f"Listening on: http://{API_HOST}:{API_PORT}")
        print(f"Logs: {MANUAL_LOG_PATH}")


def stop_server():
    """Stop the running API server"""
    pid = None

    # Try PID file first
    if os.path.exists(API_PID_FILE):
        try:
            with open(API_PID_FILE, 'r') as f:
                pid = int(f.read().strip())
            # Verify the PID is actually running
            os.kill(pid, 0)
        except (ValueError, ProcessLookupError, OSError):
            pid = None
            if os.path.exists(API_PID_FILE):
                os.remove(API_PID_FILE)

    # Fallback: find by port
    if not pid:
        pid = _find_pid_by_port()

    if not pid:
        print("API server is not running.")
        return

    print(f"Stopping API server (PID: {pid})...")

    try:
        import signal
        os.kill(pid, signal.SIGTERM)

        import time
        time.sleep(2)

        # Verify it stopped
        still_running = False
        try:
            os.kill(pid, 0)
            still_running = True
        except (ProcessLookupError, OSError):
            pass

        if still_running:
            print("Server did not stop gracefully, forcing...")
            os.kill(pid, signal.SIGKILL)

        if os.path.exists(API_PID_FILE):
            os.remove(API_PID_FILE)
        print("API server stopped successfully.")

    except Exception as e:
        print(f"Error stopping API server: {str(e)}")


def _run_server():
    """Run the uvicorn server and write PID file"""
    with open(API_PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    log(f"Starting Manual KPI Check API on {API_HOST}:{API_PORT} (PID: {os.getpid()})")
    try:
        uvicorn.run(app, host=API_HOST, port=int(API_PORT))
    finally:
        if os.path.exists(API_PID_FILE):
            os.remove(API_PID_FILE)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='KPI Manual Check API Server')
    parser.add_argument('--start', action='store_true', help='Start API server as background process')
    parser.add_argument('--stop', action='store_true', help='Stop running API server')
    parser.add_argument('--run-server', action='store_true', help=argparse.SUPPRESS)

    args = parser.parse_args()

    if args.run_server:
        _run_server()
    elif args.stop:
        stop_server()
    elif args.start:
        start_detached()
    else:
        # Default: start detached
        start_detached()
