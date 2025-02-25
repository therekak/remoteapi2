import re
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


_BARGS = re.compile(r'^(?P<lower>[(\[])(?P<upper>[)\]])$')


class Iono(object):

    async def edensdf_(self, api: APIClientASYNC, characteristics: List[str] | Tuple[str] = None, ids: pd.DataFrame = None,
        order_attrs: Optional[List[str]] | None = None, order_by: Optional[List[str]] | None = None):
        order_attrs = ['timestamp', 'station'] if order_attrs is None else order_attrs
        order_by = ['asc', ] if order_by is None else order_by
        if ids is not None:
            ids = ids.to_parquet(engine='pyarrow', index=False)
            ids = [('ids', ids)]

        return await api.df('/idb/edensdf', post=True,
            params=dict(
                start=self.start, end=self.end, stations=self.stations, characteristics=characteristics,
                order_attrs=order_attrs, order_by=order_by
            ),
            files=ids
        )

    async def obsdf_(self, api: APIClientASYNC, charcheckna: List[str] | Tuple[str] = None,
        order_attrs: Optional[List[str]] | None = None, order_by: Optional[List[str]] | None = None):
        order_attrs = ['timestamp', 'station'] if order_attrs is None else order_attrs
        order_by = ['asc', ] if order_by is None else order_by
        return await api.df('/idb/obsdf', params=dict(
            start=self.start, end=self.end, stations=self.stations, charcheckna=charcheckna, order_attrs=order_attrs, order_by=order_by)
        )

    async def df_(self, api: APIClientASYNC, characteristics: List[str] | Tuple[str] = None,
                  order_attrs: Optional[List[str]] | None = None, order_by: Optional[List[str]] | None = None):
        order_attrs = ['station', 'timestamp'] if order_attrs is None else order_attrs
        order_by = ['asc', ] if order_by is None else order_by
        return await api.df('/idb/saodf', params=dict(start=self.start, end=self.end, stations=self.stations,
                                characteristics=characteristics, order_attrs=order_attrs, order_by=order_by))


    async def istations_(self, api: APIClientASYNC):
        return await api.get('idb/istations', params=dict(start=self.start, end=self.end,
            products=['SAO',] if self.products is None else self.products),
            rtype=ReturnType.msgpack, pydmodel=schemas.StationDBO)

    def __init__(self, start: datetime = None, end: datetime = None,
                 products: Optional[List[str]] = None, stations: Optional[List[str]] = None,
                 resolution: Literal['year', 'month', 'day'] = 'year', bounds='[]'):
        _bmapper = {('lower','['): 'inclusive', ('upper',']'): 'inclusive', ('lower','('): 'exclusive', ('upper',')'): 'exclusive'}
        try:
            bounds_ = _BARGS.match(bounds).groupdict()
            bounds_ = {k: _bmapper[(k,v)] for k,v in bounds_.items()}
        except:
            raise ValueError(f'Malformed bounds attribute: {bounds}')

        start = ciso8601.parse_datetime(start) if isinstance(start, str) else (start if start else cfg['BASE_DATE'])
        if bounds_['lower'] == 'exclusive':
            start += timedelta(milliseconds=1)
        self.start = start.isoformat()

        end_ = ciso8601.parse_datetime(end) if isinstance(end, str) else (end if end else datetime.now(UTC).replace(tzinfo=None))
        if end and bounds_['upper'] == 'exclusive':
            end = end_ - timedelta(milliseconds=1)
        else:
            end = end_

        self.end = end.isoformat()

        self.products = products
        self.stations = stations
        self.resolution = resolution