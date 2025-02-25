import asyncio
import os, sys
import magic
import uuid
import io
import apyio
import orjson
from contextlib import nullcontext
from enum import Enum, IntEnum
from tqdm import tqdm
from tqdm.asyncio import tqdm as asynctqdm
import ormsgpack
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tenacity import AsyncRetrying
from typing import Union
from furl import furl
import pandas as pd

from . import _asyncu

from iapi import Logger, cfg


class ReturnType(IntEnum):
    content = 1
    ascii = 2
    json = 4
    msgpack = 8


class APIClientASYNC(_asyncu.AsyncCTXClass):

    @retry(wait=wait_exponential(multiplier=1, min=1, max=60),
           retry=retry_if_exception_type((httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, Exception)))
    async def df(self, endpoint, headers=None, params=None, data=None, json=None, post=False, files=None):
        resp = None
        content_type = None
        try:
            async with apyio.BytesIO() as bf:
                _client = await self.client()

                async with (_client.stream('GET', endpoint, headers=headers, params=params) if post is False else
                            _client.stream('POST', endpoint, headers=headers, params=params, data=data, json=json, files=files)
                ) as r:
                    if r.status_code in (422,):
                        r.raise_for_status()
                    headers_ = dict(r.headers.raw)
                    total = int(headers_[b"content-length"])
                    content_type = headers_[b"content-type"].decode('utf-8')
                    isocstream = bool(content_type == 'application/octet-stream')
                    with asynctqdm(total=total, unit_scale=True, unit_divisor=1024, unit="B") if (isocstream and self.verbose) else nullcontext() as p:
                        num_bytes_downloaded = r.num_bytes_downloaded
                        async for chunk in r.aiter_bytes():
                            bf.write(chunk)
                            if p:
                                p.update(r.num_bytes_downloaded - num_bytes_downloaded)
                            num_bytes_downloaded = r.num_bytes_downloaded
                await bf.seek(0)
                if isocstream:
                    resp = pd.read_parquet(bf._stream, engine='pyarrow')
                else:
                    resp = await bf.getvalue()

            if isocstream:
                resp['id'] = resp['id'].apply(lambda _: uuid.UUID(_))
            else:
                resp = orjson.loads(resp.decode('utf-8'))

        except httpx.ConnectError as e:
            Logger.logger.critical(f'Upon request Connection Error: {e}')
            raise e
        except httpx.ConnectTimeout as e:
            Logger.logger.critical(f'Upon request Connection Timeout: {e}')
            raise e
        except httpx.ReadTimeout as e:
            Logger.logger.critical(f'Upon request Read Timeout: {e}')
            raise e
        except httpx.HTTPStatusError as e:
            raise e
        except Exception as e:
            Logger.logger.error(f'Unable to complete API request {e}')
            raise httpx.RequestError(e.__str__())

        return resp

    @retry(wait=wait_exponential(multiplier=1, min=1, max=60),
           retry=retry_if_exception_type((httpx.ConnectError, httpx.ConnectTimeout)))
    async def get(self, endpoint, headers=None, params=None, rtype: ReturnType=None, pydmodel=None):
        resp = None
        msgpack_ = rtype and ReturnType.msgpack==rtype
        if msgpack_:
            headers = headers | {"accept": "application/x-msgpack"} if headers else {"accept": "application/x-msgpack"}
        try:
            _client = await self.client()
            r = await _client.get(endpoint, headers=headers, params=params)
            r.raise_for_status()
            if rtype is None:
                try:
                    headers_ = dict(r.headers.raw)
                    ctype = headers_.get(b'content-type', '').decode('utf-8')
                    rtype = ReturnType.json if ctype == 'application/json' else ReturnType.content
                    msgpack_ = ctype=="application/x-msgpack"
                except:
                    rtype = ReturnType.content

            if msgpack_:
                content = ormsgpack.unpackb(r.content)
            else:
                content = r.content

            if rtype==ReturnType.json:
                resp = orjson.loads(content.decode('utf-8'))
            elif rtype==ReturnType.ascii:
                resp = content.decode('utf-8')
            else:
                resp = content

            if pydmodel and rtype in (ReturnType.json, ReturnType.msgpack):
                if isinstance(resp, list):
                    resp_ = [pydmodel(**_) for _ in resp]
                else:
                    resp_ = pydmodel(**resp)

                resp = resp_
        except httpx.ConnectError as e:
            Logger.logger.critical(f'Upon request Connection Error: {e}')
            raise e
        except httpx.ConnectTimeout as e:
            Logger.logger.critical(f'Upon request Connection Timeout: {e}')
            raise e
        except httpx.RemoteProtocolError as e:
            Logger.logger.critical(f'Upon request Connection Timeout: {e}')
            raise e
        except httpx.ReadTimeout as e:
            Logger.logger.critical(f'Upon request Read Timeout: {e}')
            raise e
        except httpx.HTTPStatusError as e:
            raise e
        except Exception as e:
            import traceback
            err = traceback.format_exc()
            Logger.logger.error(f'B[A] Unable to complete API request {err}')
            raise httpx.RequestError(e.__str__())

        return resp

    @retry(wait=wait_exponential(multiplier=1, min=1, max=60),
           retry=retry_if_exception_type((httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, Exception)))
    async def client(self):
        if not self._client:
            try:
                transport = httpx.AsyncHTTPTransport(retries=3, verify=False)
                timeout = httpx.Timeout(self._timeout, read=self._timeout)
                self._client = httpx.AsyncClient(transport=transport, base_url=self.uri.url, verify=False,
                        headers=self._headers, params=self._params, timeout=timeout)
            except httpx.ConnectError as e:
                Logger.logger.critical(f'API Client Connection Error: {e}')
                raise e
            except httpx.ConnectTimeout as e:
                Logger.logger.critical(f'API Client Connection Timeout: {e}')
                raise e
            except httpx.ReadTimeout as e:
                Logger.logger.critical(f'Upon request Read Timeout: {e}')
                raise e
            except Exception as e:
                Logger.logger.error(f'Unable to connect to API {e}')
                raise httpx.RequestError(e.__str__())

        return self._client

    def __init__(self, uri: Union[str, furl], headers=None, params=None, timeout=120, loop: _asyncu.Loop = None, verbose=False, _logprefix=None):
        super().__init__(loop=loop, _logprefix=_logprefix)
        self.uri = uri
        self._client = None
        self._headers = headers
        self._params = params
        self._timeout = timeout
        self.verbose = verbose

    async def disconnect(self):
        _client = None
        if self._client:
            try:
                _client = await self.client()
            except Exception as e:
                Logger.logger.critical(f'Error while disconnecting {self.uri} API')
            finally:
                try:
                    await _client.aclose()
                except:
                    pass

    def close(self):
        if (not self.loop_ or not self.loop_.started) or self.loop.is_closed():
            return
        try:
            self.loop.run_until_complete(self.disconnect())
        except Exception as e:
            pass
        finally:
            pass

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


class APIClient(object):

    @retry(wait=wait_exponential(multiplier=1, min=1, max=60),
           retry=retry_if_exception_type((httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout)))
    def df(self, endpoint, headers=None, params=None):
        resp = None
        content_type = None
        try:
            with io.BytesIO() as bf:
                with self.client.stream('GET', endpoint, headers=headers, params=params) as r:
                    headers_ = dict(r.headers.raw)
                    total = int(headers_[b"content-length"])
                    content_type = headers_[b"content-type"].decode('utf-8')
                    isocstream = bool(content_type=='application/octet-stream')
                    with tqdm(total=total, unit_scale=True, unit_divisor=1024, unit="B") if (isocstream and self.verbose) else nullcontext() as p:
                        num_bytes_downloaded = r.num_bytes_downloaded
                        for chunk in r.iter_bytes():
                            bf.write(chunk)
                            if p:
                                p.update(r.num_bytes_downloaded - num_bytes_downloaded)
                            num_bytes_downloaded = r.num_bytes_downloaded
                bf.seek(0)
                if isocstream:
                    resp = pd.read_parquet(bf, engine='pyarrow')
                else:
                    resp = bf.getvalue()

            if isocstream:
                resp['id'] = resp['id'].apply(lambda _: uuid.UUID(_))
            else:
                resp = orjson.loads(resp.decode('utf-8'))

        except httpx.ConnectError as e:
            Logger.logger.critical(f'Upon request Connection Error: {e}')
            raise e
        except httpx.ConnectTimeout as e:
            Logger.logger.critical(f'Upon request Connection Timeout: {e}')
            raise e
        except httpx.ReadTimeout as e:
            Logger.logger.critical(f'Upon request Read Timeout: {e}')
            raise e
        except Exception as e:
            Logger.logger.error(f'C Unable to complete API request {e}')
            raise httpx.RequestError(e.__str__())

        return resp

    @retry(wait=wait_exponential(multiplier=1, min=1, max=60),
           retry=retry_if_exception_type((httpx.ConnectError, httpx.ConnectTimeout)))
    def get(self, endpoint, headers=None, params=None, rtype: ReturnType=None, pydmodel=None):
        resp = None
        msgpack_ = rtype and ReturnType.msgpack==rtype
        if msgpack_:
            headers = headers | {"accept": "application/x-msgpack"} if headers else {"accept": "application/x-msgpack"}
        try:
            r = self.client.get(endpoint, headers=headers, params=params)
            r.raise_for_status()
            if rtype is None:
                try:
                    headers_ = dict(r.headers.raw)
                    ctype = headers_.get(b'content-type', '').decode('utf-8')
                    rtype = ReturnType.json if ctype=='application/json' else ReturnType.content
                    msgpack_ = ctype=="application/x-msgpack"
                except:
                    rtype = ReturnType.content

            if msgpack_:
                content = ormsgpack.unpackb(r.content)
            else:
                content = r.content

            if rtype==ReturnType.json:
                resp = orjson.loads(content.decode('utf-8'))
            elif rtype==ReturnType.ascii:
                resp = content.decode('utf-8')
            else:
                resp = content

            if pydmodel and rtype in (ReturnType.json, ReturnType.msgpack):
                if isinstance(resp, list):
                    resp_ = [pydmodel(**_) for _ in resp]
                else:
                    resp_ = pydmodel(**resp)

                resp = resp_
        except httpx.ConnectError as e:
            Logger.logger.critical(f'Upon request Connection Error: {e}')
            raise e
        except httpx.ConnectTimeout as e:
            Logger.logger.critical(f'Upon request Connection Timeout: {e}')
            raise e
        except Exception as e:
            Logger.logger.error(f'D Unable to complete API request {e}')
            raise httpx.RequestError(e.__str__())

        return resp

    @property
    @retry(wait=wait_exponential(multiplier=1, min=1, max=60),
           retry=retry_if_exception_type((httpx.ConnectError, httpx.ConnectTimeout)))
    def client(self):
        if not self._client:
            try:
                transport = httpx.HTTPTransport(retries=3)
                timeout = httpx.Timeout(self._timeout, read=self._timeout)
                self._client = httpx.Client(transport=transport, base_url=self.uri.url, verify=False,
                    headers=self._headers, params=self._params, timeout=timeout)
            except httpx.ConnectError as e:
                Logger.logger.critical(f'API Client Connection Error: {e}')
                raise e
            except httpx.ConnectTimeout as e:
                Logger.logger.critical(f'API Client Connection Timeout: {e}')
                raise e
            except Exception as e:
                Logger.logger.error(f'Unable to connect to API {e}')
                raise httpx.RequestError(e.__str__())

        return self._client

    def __init__(self, uri: Union[str, furl], headers=None, params=None, timeout=30, verbose=False, _logprefix=None):
        self.uri = uri

        self._client = None
        self._headers = headers
        self._params = params
        self._timeout = timeout
        self.verbose = verbose
        self._logprefix = _logprefix

    def disconnect(self):
        _client = None
        if self._client:
            try:
                _client = self.client
            except:
                Logger.logger.critical(f'Error while disconnecting {self.uri} API')
            finally:
                try:
                    _client.close()
                except:
                    pass

    def close(self):
        try:
            self.disconnect()
        except Exception as e:
            pass
        finally:
            pass

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()