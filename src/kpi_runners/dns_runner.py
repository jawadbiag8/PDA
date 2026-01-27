import socket
import time
from src.kpi_runners.base import BaseKPIRunner
from urllib.parse import urlparse

class DNSKPIRunner(BaseKPIRunner):

    def run(self):
        url = self.asset['url']
        hostname = urlparse(url).netloc or url.replace('https://', '').replace('http://', '').split('/')[0]

        try:
            start_time = time.time()
            ip_address = socket.gethostbyname(hostname)
            ip_addresses = socket.gethostbyname_ex(hostname)[2]
            resolution_time = (time.time() - start_time) * 1000  # Convert to milliseconds

            detail_msg = f"Hostname: {hostname}, Primary IP: {ip_address}"
            if len(ip_addresses) > 1:
                detail_msg += f", Total IPs: {len(ip_addresses)} ({', '.join(ip_addresses)})"
            detail_msg += f", Resolution time: {resolution_time:.0f}ms"

            return {
                "flag": False,  # No problem - DNS resolved successfully
                "value": ip_address,
                "details": detail_msg
            }

        except socket.gaierror as e:
            return {
                "flag": True,  # Problem - DNS resolution failed
                "value": None,
                "details": f"DNS resolution failed: {str(e)}"
            }
        except Exception as e:
            return {
                "flag": True,  # Problem - unexpected error
                "value": None,
                "details": f"Error: {str(e)}"
            }