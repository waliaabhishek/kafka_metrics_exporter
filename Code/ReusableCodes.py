import time
from functools import reduce
import requests
import concurrent.futures
import asyncio
import itertools
from requests.auth import HTTPBasicAuth
from urllib3.exceptions import InsecureRequestWarning
from urllib.parse import urlparse
from url_normalize import url_normalize


def current_milli_time():
    import time
    return int(round(time.time() * 1000))


def flatten(d, pref=''):
    return(reduce(
        lambda new_d, kv:
            isinstance(kv[1], dict) and {
                **new_d, **flatten(kv[1], pref + kv[0])} or {**new_d, pref + kv[0]: kv[1]},
            d.items(),
            {}))


async def invoke_urls_concurrently(input_urls: list, worker_count=10, **kwargs):
    # Async Thread executor for making calls to the REST API.
    # The input_uri takes a list of uri as an argument to call and calls them parallely
    # The result is returned back as a list of data responses.
    return_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_url = (executor.submit(invoke_url,
                                         url, kwargs=kwargs) for url in input_urls)
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                contents = future.result()
                return_data.append(contents)
            except Exception:
                raise
    return return_data


# Invoke the call for the input_uri passed.
# This will setup the request session and calls the url.
# Also does basic auth if AUTH_ENABLED is True.
def invoke_url(input_url, **kwargs):
    session = requests.Session()
    session.verify = kwargs.get('verify', False)
    auth_type = kwargs.get('auth_type', "None")
    # Suppress only the single warning from urllib3 needed.
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    if "basic" == auth_type:
        session.auth = HTTPBasicAuth(kwargs['user'], kwargs['password'])
    else:
        session.auth = None
    try:
        contents = session.get(url_normalize(input_url))
        if contents.ok:
            return contents.json()
    except Exception:
        raise
