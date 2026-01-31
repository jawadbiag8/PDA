import requests
import ssl
from src.kpi_runners.base import BaseKPIRunner
from src.config.settings import DEFAULT_TIMEOUT, FLAPPING_TIMEOUT
from urllib3.util.ssl_ import create_urllib3_context
from requests.adapters import HTTPAdapter
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class GovernmentSSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = context
        kwargs['assert_hostname'] = False
        return super().init_poolmanager(*args, **kwargs)


class HttpKPIRunner(BaseKPIRunner):

    def run(self):
        url = self.asset['url']
        kpi_name = self.kpi.get('kpi_name', '').lower()

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }

        session = requests.Session()
        session.headers.update(headers)
        session.mount('https://', GovernmentSSLAdapter())

        try:
            response = session.get(url, timeout=DEFAULT_TIMEOUT, verify=False)
            response_time = response.elapsed.total_seconds()
            
            # Website completely down (no response)
            if 'completely down' in kpi_name:
                return {
                    "flag": False,  # No problem - site is UP
                    "value": response.status_code,
                    "details": f"Site is UP - Status: {response.status_code}, Response time: {response_time:.2f}s, Server: {response.headers.get('Server', 'Unknown')}"
                }

            # Hosting/network outage
            elif 'hosting' in kpi_name or 'network outage' in kpi_name:
                content_length = len(response.content) if response.content else 0
                return {
                    "flag": False,  # No problem - site is accessible
                    "value": response.status_code,
                    "details": f"Site accessible - Status: {response.status_code}, Response time: {response_time:.2f}s, Content size: {content_length} bytes"
                }
            
            # Intermittent availability (flapping)
            elif 'intermittent' in kpi_name or 'flapping' in kpi_name:
                # Check multiple times to detect flapping
                import time
                attempts = 3
                failures = 0
                success_times = []
                failure_reasons = []

                for i in range(attempts):
                    try:
                        start = time.time()
                        resp = session.get(url, timeout=FLAPPING_TIMEOUT, verify=False)
                        elapsed = time.time() - start
                        success_times.append(elapsed)
                    except requests.exceptions.Timeout:
                        failures += 1
                        failure_reasons.append(f"attempt {i+1}: timeout")
                    except requests.exceptions.ConnectionError:
                        failures += 1
                        failure_reasons.append(f"attempt {i+1}: connection error")
                    except Exception as e:
                        failures += 1
                        failure_reasons.append(f"attempt {i+1}: {type(e).__name__}")

                is_flapping = failures > 0
                avg_time = sum(success_times) / len(success_times) if success_times else 0

                detail_msg = f"Tested {attempts} times: {attempts - failures} successful"
                if success_times:
                    detail_msg += f" (avg {avg_time:.2f}s)"
                if failures > 0:
                    detail_msg += f", {failures} failed"
                    if failure_reasons:
                        detail_msg += f" - [{', '.join(failure_reasons[:2])}]"

                return {
                    "flag": is_flapping,  # True if any failures detected
                    "value": f"{failures}/{attempts}",
                    "details": detail_msg
                }
            
            # Backend response time
            elif 'backend response' in kpi_name or 'response time' in kpi_name:
                slow_threshold = 3.0  # Consider slow if > 3 seconds
                return {
                    "flag": response_time > slow_threshold,  # True if slow
                    "value": round(response_time, 2),
                    "details": f"Response time: {response_time:.2f}s ({'SLOW' if response_time > slow_threshold else 'OK'})"
                }
            
            # Website not using HTTPS
            elif 'not using https' in kpi_name or 'https' in kpi_name:
                is_https = url.startswith('https://')
                return {
                    "flag": not is_https,  # True if NOT using HTTPS
                    "value": 'HTTPS' if is_https else 'HTTP',
                    "details": f"Protocol: {'HTTPS ✓' if is_https else 'HTTP ✗'}"
                }
            
            # Default HTTP check
            else:
                return {
                    "flag": response.status_code >= 400,  # True if error status
                    "value": response.status_code,
                    "details": f"Status: {response.status_code}, Time: {response_time:.2f}s"
                }

        except requests.exceptions.Timeout:
            return {
                "flag": True,  # Problem - timeout occurred
                "value": None,
                "details": "Request timeout"
            }
        except requests.exceptions.ConnectionError:
            return {
                "flag": True,  # Problem - connection failed
                "value": None,
                "details": "Connection error - site may be down"
            }
        except requests.exceptions.RequestException as e:
            return {
                "flag": True,  # Problem - request failed
                "value": None,
                "details": str(e)
            }