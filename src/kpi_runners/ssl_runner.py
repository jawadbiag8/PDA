import ssl
import socket
from datetime import datetime, timezone
from src.kpi_runners.base import BaseKPIRunner
from urllib.parse import urlparse
from cryptography import x509
from cryptography.hazmat.backends import default_backend

class SSLKPIRunner(BaseKPIRunner):

    def run(self):
        url = self.asset['url']
        hostname = urlparse(url).netloc or url.replace('https://', '').replace('http://', '').split('/')[0]

        # Create context that doesn't validate certificates
        # This allows us to inspect certificates even if they're from pakistan.gov.pk (local issuer)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        try:
            with socket.create_connection((hostname, 443), timeout=10) as sock:
                with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                    # Get certificate in binary DER format
                    cert_der = ssock.getpeercert(binary_form=True)

                    if not cert_der:
                        return {
                            "flag": True,
                            "value": None,
                            "details": "No certificate found"
                        }

                    # Parse the certificate using cryptography library
                    cert = x509.load_der_x509_certificate(cert_der, default_backend())

                    # Extract certificate information
                    expiry = cert.not_valid_after_utc

                    # Get certificate CN (Common Name)
                    cert_cn = "Unknown"
                    try:
                        cn_attributes = cert.subject.get_attributes_for_oid(x509.oid.NameOID.COMMON_NAME)
                        if cn_attributes:
                            cert_cn = cn_attributes[0].value
                    except Exception:
                        pass

                    # Get issuer CN
                    issuer_cn = "Unknown"
                    try:
                        issuer_attributes = cert.issuer.get_attributes_for_oid(x509.oid.NameOID.COMMON_NAME)
                        if issuer_attributes:
                            issuer_cn = issuer_attributes[0].value
                    except Exception:
                        pass

                    # Calculate days until expiry
                    days_until_expiry = (expiry - datetime.now(timezone.utc)).days
                    is_expired = days_until_expiry < 0
                    expiring_soon = 0 < days_until_expiry <= 30  # Warning if expires within 30 days

                    return {
                        "flag": is_expired,  # True if certificate is expired
                        "value": expiry.strftime('%Y-%m-%d'),
                        "details": f"CN: {cert_cn}, Issuer: {issuer_cn}, Expires in {days_until_expiry} days ({'EXPIRED' if is_expired else 'WARNING' if expiring_soon else 'OK'})"
                    }

        except Exception as e:
            return {
                "flag": True,  # Problem - SSL check failed
                "value": None,
                "details": f"SSL connection failed: {str(e)}"
            }
