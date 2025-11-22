import inspect

import httpx
import orjson
import zstandard as zstd
from httpx import (
    CloseError,
    ConnectError,
    ConnectTimeout,
    CookieConflict,
    DecodingError,
    HTTPError,
    HTTPStatusError,
    InvalidURL,
    LocalProtocolError,
    NetworkError,
    PoolTimeout,
    ProtocolError,
    ProxyError,
    ReadError,
    ReadTimeout,
    RemoteProtocolError,
    RequestError,
    TimeoutException,
    TooManyRedirects,
    TransportError,
    UnsupportedProtocol,
    WriteError,
    WriteTimeout,
)
from httpx._client import ClientState
from httpx._decoders import SUPPORTED_DECODERS, ContentDecoder

httpx.Response.json = lambda self: orjson.loads(self.content)
_HAS_PROXIES_PARAM = "proxies" in inspect.signature(httpx.AsyncClient.__init__).parameters


class ZstdDecoder(ContentDecoder):
    def __init__(self) -> None:
        self.decompressor = zstd.ZstdDecompressor().decompressobj()

    def decode(self, data: bytes) -> bytes:
        # TODO: optimization
        if not data:
            return b""
        data_parts = [self.decompressor.decompress(data)]
        while self.decompressor.eof and self.decompressor.unused_data:
            unused_data = self.decompressor.unused_data
            self.decompressor = zstd.ZstdDecompressor().decompressobj()
            data_parts.append(self.decompressor.decompress(unused_data))
        return b"".join(data_parts)

    def flush(self) -> bytes:
        ret = self.decompressor.flush()
        if not self.decompressor.eof:
            raise DecodingError("Zstandard data is incomplete")
        return ret


SUPPORTED_DECODERS["zstd"] = ZstdDecoder

DEFAULT_TIMEOUT = 45


def _proxy_kwargs(proxy_or_map):
    """
    Build proxy kwargs compatible with both httpx <0.28 (proxies) and >=0.28 (proxy).
    """
    if _HAS_PROXIES_PARAM:
        return {"proxies": proxy_or_map}
    proxy = None
    if isinstance(proxy_or_map, dict):
        proxy = (
            proxy_or_map.get("all://")
            or proxy_or_map.get("https")
            or proxy_or_map.get("http")
            or proxy_or_map.get("https://")
            or proxy_or_map.get("http://")
        )
        if proxy is None and proxy_or_map:
            proxy = next(iter(proxy_or_map.values()))
    else:
        proxy = proxy_or_map
    return {"proxy": proxy}


async def request(method, url, proxy=None, proxies=None, **kwargs):
    if "timeout" not in kwargs:
        kwargs["timeout"] = DEFAULT_TIMEOUT
    client_kwargs = {"verify": False, "follow_redirects": True}
    client_kwargs.update(_proxy_kwargs(proxies if proxies is not None else proxy))
    async with httpx.AsyncClient(**client_kwargs) as client:
        return await client.request(method, url, **kwargs)


class Session:
    def __init__(self):
        self.headers = {}
        self.verify = False
        self._client = None
        self._proxies = None

    @property
    def cookies(self):
        return self._client.cookies.jar

    def cookies_dict(self):
        return {c.name: c.value for c in self._client.cookies.jar}

    def set_cookies(self, d):
        for k, v in d.items():
            self._client.cookies.set(k, v)

    @property
    def proxies(self):
        return self._proxies

    @proxies.setter
    def proxies(self, p):
        self._proxies = p
        self._set_client()

    def _set_client(self):
        client_kwargs = {"verify": self.verify, "follow_redirects": True}
        client_kwargs.update(_proxy_kwargs(self._proxies))
        self._client = httpx.AsyncClient(**client_kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args, **kwargs):
        await self._close()

    async def _close(self):
        if self._client and self._client._state is ClientState.OPENED:
            await self._client.__aexit__()

    async def request(self, *args, headers=None, proxies=None, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = DEFAULT_TIMEOUT
        if self._client._state is ClientState.UNOPENED:
            await self._client.__aenter__()
        headers = self.headers | (headers or {})
        headers = {k: v for k, v in headers.items() if v is not None}
        kwargs = {k: v for k, v in kwargs.items() if v}
        if proxies is not None:
            kwargs.update(_proxy_kwargs(proxies))
        return await self._client.request(*args, headers=headers, **kwargs)

    async def get(self, *args, **kwargs):
        return await self.request("get", *args, **kwargs)

    async def post(self, *args, **kwargs):
        return await self.request("post", *args, **kwargs)


__all__ = [
    "HTTPError",
    "RequestError",
    "TransportError",
    "TimeoutException",
    "ConnectTimeout",
    "ReadTimeout",
    "WriteTimeout",
    "PoolTimeout",
    "NetworkError",
    "ConnectError",
    "ReadError",
    "WriteError",
    "CloseError",
    "ProtocolError",
    "LocalProtocolError",
    "RemoteProtocolError",
    "ProxyError",
    "UnsupportedProtocol",
    "DecodingError",
    "TooManyRedirects",
    "HTTPStatusError",
    "InvalidURL",
    "CookieConflict",
    "ZstdDecoder",
    "request",
    "Session",
]
