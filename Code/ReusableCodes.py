import time
from functools import reduce
import asyncio
from url_normalize import url_normalize
import aiohttp
import traceback

loop = asyncio.get_event_loop()
session = aiohttp.ClientSession(loop=loop)


def current_milli_time():
    return int(round(time.time() * 1000))


def flatten(d, pref=''):
    return(reduce(
        lambda new_d, kv:
            isinstance(kv[1], dict) and {
                **new_d, **flatten(kv[1], pref + kv[0])} or {**new_d, pref + kv[0]: kv[1]},
            d.items(),
            {}))


async def fetch(session, url, **kwargs):
    parse_response = kwargs.get('parse_response', False)
    async with session.get(url) as resp:
        try:
            print("Received response from " + str(resp.url) +
                  ". Status Code: " + str(resp.status))
            assert resp.status == 200
            return await resp.json()
        except Exception:
            if (parse_response):
                print("Exception in parsing response from URL " +
                      url + ". Response Received: " + await resp.text())
            else:
                print("Exception in parsing response from URL " +
                      url + ".")


async def post(session, url):
    async with session.post(url) as resp:
        print("Received response from " + resp.url +
              ". Status Code: " + resp.status)
        return await resp.text()


async def call_http_async(url_list: list, method="get", **kwargs):
    global loop
    enable_traceback = kwargs.get('traceback', False)
    for url in url_list:
        url = url_normalize(url)
        print("Calling " + str(method) + " for URL: " + url)
        try:
            if method == "get":
                return await fetch(session, url)
            elif method == "post":
                return await post(session, url)
        except Exception as ex:
            print(ex)
            if (enable_traceback):
                print(''.join(traceback.format_exception(
                    etype=type(ex), value=ex, tb=ex.__traceback__)))


if __name__ == "__main__":
    http_test_urls = ["www.python.org", "www.facebook.com", "www.google.com"]
    # http_test_urls = ["http://www.py33thon.org", ]
    asyncio.ensure_future(call_http_async(http_test_urls))
    loop.run_forever()
