import ssl
import certifi
from urllib3.util.ssl_ import create_urllib3_context
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

class GovernmentSSLAdapter(HTTPAdapter):
    """
    Custom SSL adapter that accepts certificates issued to pakistan.gov.pk
    for all Pakistani government websites
    """
    
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        context.load_verify_locations(certifi.where())
        
        # Set check_hostname to False to allow pakistan.gov.pk cert for subdomains
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED
        
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)