import re
import uuid
import httpx
from typing import Literal, Optional, List, Tuple
from datetime import datetime, timedelta, UTC

import json
import ciso8601
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.exc
from sqlalchemy.dialects import postgresql

from iapi import Logger, cfg

from . import schemas
from .api import APIClient, APIClientASYNC, ReturnType


class TTIDE(object):

    async def data_(self, api: APIClientASYNC, navmeta):
        urimapper_ = dict(
            fof2 = '/products/eis/data/fof2ncascii/:pubid:',
            hmf2 = '/products/noa/data/hmf2/:pubid:'
        )

        try:
            uri = urimapper_[navmeta['type']].replace(':pubid:', str(navmeta['uuid']))
            resp = await api.get(uri, params=None, rtype=ReturnType.ascii)
            return resp
        except httpx.HTTPStatusError:
            Logger.logger.critical(f'No {urimapper_[type]} datasets available @ {self.timestamp}')
            return None


    async def nav_(self, api: APIClientASYNC, type: Literal['fof2', 'hmf2']):
        urimapper_ = dict(
            fof2 = '/products/eis/list/nav',
            hmf2 = '/products/noa/list/nav'
        )
        prodmapper_ = dict(
            fof2 = 'DIASNC.fof2ncascii',
            hmf2 = 'TAD2D.hmf2'
        )

        if self.timestamp=='LAST':
            kwargs = dict(exact = 'false', nav=self.timestamp, product = prodmapper_[type])
        else:
            kwargs = dict(date = self.timestamp, exact = 'false', product = prodmapper_[type])

        try:
            resp = await api.get(urimapper_[type], params=kwargs, rtype=ReturnType.json)
        except httpx.HTTPStatusError:
            Logger.logger.critical(f'No {urimapper_[type]} metadata available @ {self.timestamp}')
            return dict(type=type, timestamp=None, uuid=None)


        rpubid = resp['data']['pubid']
        rtimestamp = ciso8601.parse_datetime(resp['data']['pstart'])
        if self.timestamp_ is not None:
            tdelta = abs(rtimestamp - self.timestamp_)
            if tdelta > self.maxtdev:
                Logger.logger.critical(
                    f'Too large timedelta ({tdelta}): Product {prodmapper_[type]} timestamp @ {rtimestamp.isoformat()}, requested @ {self.timestamp_}'
                )
                return dict(type=type, timestamp=None, uuid=None)

        return dict(type=type, timestamp= rtimestamp, uuid= uuid.UUID(rpubid))


    def __init__(self, timestamp: Optional[datetime] = None, lat: float = None, lon: float = None, maxtdev=timedelta(minutes=60)):

        self.timestamp_ = timestamp = ciso8601.parse_datetime(timestamp) if isinstance(timestamp, str) else timestamp
        self.timestamp = timestamp.isoformat() if self.timestamp_ else 'LAST'

        self.maxtdev = maxtdev

        self.lat = lat
        self.lon = lon