import asyncio
from yarl import URL
import aiohttp
from aiohttp.client_exceptions import InvalidURL
from aiohttp.helpers import sentinel, proxies_from_env


class Session:
    def __init__(self, **kwargs):
        self._session_obj = None
        self._kwargs = kwargs

    @property
    def session_obj(self):
        if self._session_obj is None:
            if 'loop' not in self._kwargs:
                loop = asyncio.get_running_loop()
                self._kwargs['loop'] = loop
            self._session_obj = _ClientSession(**self._kwargs)
        return self._session_obj

    def __getattr__(self, attr):
        return getattr(self.session_obj, attr)


class _ClientSession(aiohttp.ClientSession):
    def __init__(self, *, trust_env=True, timeout=sentinel, **kwargs):
        if timeout is sentinel:
            timeout = aiohttp.ClientTimeout(total=20)
        return super().__init__(trust_env=trust_env, timeout=timeout, **kwargs)

    async def _request(self, method, str_or_url, *,
                       proxy=None, proxy_auth=None, **kwargs):
        if proxy is None and self._trust_env:
            try:
                url = URL(str_or_url)
            except ValueError:
                raise InvalidURL(str_or_url)
            proxy, proxy_auth = self._proxy_from_env(url.scheme)
        resp = await super()._request(
            method,
            str_or_url,
            proxy=proxy,
            proxy_auth=proxy_auth,
            **kwargs)
        return resp

    @staticmethod
    def _proxy_from_env(scheme):
        if scheme == 'wss':
            scheme = 'https'
        if scheme == 'ws':
            scheme = 'http'
        proxies = proxies_from_env()
        proxy_info = proxies.get(scheme)
        if proxy_info:
            return proxy_info.proxy, proxy_info.proxy_auth
        else:
            return None, None
