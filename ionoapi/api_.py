import os, sys
from datetime import datetime

import apiclient.authentication_methods
import ciso8601
from prodict import Prodict
from typing import TYPE_CHECKING, List, Dict, Optional, Union, Callable
from apiclient import APIClient

from apiclient.response_handlers import RequestsResponseHandler, JsonResponseHandler
from apiclient.request_formatters import JsonRequestFormatter
from apiclient.exceptions import APIClientError, UnexpectedError

if TYPE_CHECKING:  # pragma: no cover
    # Stupid way of getting around cyclic imports when
    # using typehinting.
    from apiclient import APIClient

from iapi import Logger, cfg

from .helpers import msc_retry, urljoin, endpoint, HeaderAuthenticationJWT, RequestStrategyU, NoAuthentication


class ISTREAMAPI(APIClient):
    _NEXT_REQUEST = None
    _MIN_REQINTRVL = 0.0025

    @property
    def token_expired(self):
        if not isinstance(self._authentication_method,apiclient.authentication_methods.NoAuthentication):
            return self._authentication_method.expired
        else:
            return False

    @staticmethod
    def _formatDatetime(dt):
        dt_ = dt.date().strftime('%Y-%m-%d')
        tm_ = dt.time().strftime('%H:%M')
        return dt_ ,tm_


    @msc_retry
    @endpoint('idb', 'sao', 'pager', base=cfg['ISTREAMAPI']['BASE_API'])
    def _imeta(self, start: datetime, end: datetime, stations: List[str], characteristics: Optional[List[str]] = None,
               page: Optional[int]=1, size: Optional[int]=50, url=None):
        try:
            resp = self.get(url, params=dict(start=start.strftime('%Y-%m-%dT%H:%M:%S'),
                                             end=end.strftime('%Y-%m-%dT%H:%M:%S'), stations=stations,
                                             characteristics = characteristics,
                                             order_attrs='timestamp', order_by='asc', page=page, size=size))
        except APIClientError as e:
            Logger.logger.error(f'Users[GET] error {e}')
            raise
        return resp

    def __init__(self):
        self.users: Optional[Prodict] = None

        auth_obj = HeaderAuthenticationJWT(
            username=cfg['ISTREAMAPI']['USER'],
            password=cfg['ISTREAMAPI']['PASS'],
            auth_url=urljoin('token', base=cfg['ISTREAMAPI']['BASE_API'].url).url,
            parameter="Authorization",
            scheme="Bearer"
        ) if cfg['TECHTIDEAPI']['USER'] else NoAuthentication()
        super(ISTREAMAPI, self).__init__(authentication_method=auth_obj,
                                       response_handler=JsonResponseHandler,
                                       request_formatter=JsonRequestFormatter, request_strategy=RequestStrategyU())

        self.__setattr__('imeta',
                         Prodict.from_dict({'list': self._imeta, }))

    def disconnect(self):
        try:
            if self._session:
                self._session.close()
        except Exception as e:
            pass
        finally:
            self._session = None

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disconnect()


class TECHTIDEAPI(APIClient):
    _NEXT_REQUEST = None
    _MIN_REQINTRVL = 0.0025

    @property
    def token_expired(self):
        if not isinstance(self._authentication_method,apiclient.authentication_methods.NoAuthentication):
            return self._authentication_method.expired
        else:
            return False

    @staticmethod
    def _formatDatetime(dt):
        dt_ = dt.date().strftime('%Y-%m-%d')
        tm_ = dt.time().strftime('%H:%M')
        return dt_,tm_


    @msc_retry
    @endpoint('products','ionosondes','meta', base=cfg['TECHTIDEAPI']['BASE_API'])
    def _imeta(self, date_from: datetime, date_to: datetime, provider: str, url=None):
        try:
            resp = self.get(url, params=dict(date_from=date_from.strftime('%Y-%m-%dT%H:%M:%S'), date_to=date_to.strftime('%Y-%m-%dT%H:%M:%S'), provider=provider))
        except APIClientError as e:
            Logger.logger.error(f'Users[GET] error {e}')
            raise
        return resp

    def __init__(self):
        self.users: Optional[Prodict] = None

        auth_obj = HeaderAuthenticationJWT(
                                        username=cfg['TECHTIDEAPI']['USER'],
                                        password=cfg['TECHTIDEAPI']['PASS'],
                                        auth_url=urljoin('auth/login', base=cfg['TECHTIDEAPI']['BASE_API']).url,
                                        parameter="Authorization",
                                        scheme="Bearer"
                                    ) if cfg['TECHTIDEAPI']['USER'] else NoAuthentication()
        super(TECHTIDEAPI, self).__init__(authentication_method=auth_obj,
                                        response_handler=JsonResponseHandler,
                                        request_formatter=JsonRequestFormatter, request_strategy=RequestStrategyU())

        self.__setattr__('imeta',
                         Prodict.from_dict({'list': self._imeta, }))

    def disconnect(self):
        try:
            if self._session:
                self._session.close()
        except Exception as e:
            pass
        finally:
            self._session = None

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.disconnect()
