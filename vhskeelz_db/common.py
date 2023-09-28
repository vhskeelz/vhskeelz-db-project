from urllib3.util import Retry
from requests import Session
from requests.adapters import HTTPAdapter


def requests_session_retry(total=10, backoff_factor=2, backoff_max=60, status_forcelist=(502, 503, 504), **kwargs):
    s = Session()
    retries = Retry(
        total=total,
        backoff_factor=backoff_factor,
        backoff_max=backoff_max,
        status_forcelist=status_forcelist,
        **kwargs
    )
    s.mount('https://', HTTPAdapter(max_retries=retries))
    s.mount('http://', HTTPAdapter(max_retries=retries))
    return s
