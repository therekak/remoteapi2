import os,sys, re
import asyncio
import json
from io import StringIO

import ssl
import httpx

ssl._create_default_https_context = ssl._create_unverified_context
from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
disable_warnings(InsecureRequestWarning)

sys.path.insert(0, os.path.abspath('.'))
import logging
import logging.handlers
from pathlib import Path
import yaml
from furl import furl
import argparse
import ciso8601
from datetime import datetime, timedelta, UTC
from typing import List, Optional
import portion as P
from copy import copy
import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm

from ionoapi import _asyncu
from ionoapi import api

def join(loader, node):
    seq = loader.construct_sequence(node)
    return ''.join([str(i) for i in seq])


yaml.add_constructor('!join', join)
F = os.path.abspath(__file__)

_BARGS = re.compile(r'^(?P<lower>[(\[])(?P<upper>[)\]])$')

class IApiConn(_asyncu.AsyncCTXClass):

    def stop(self):
        if self.loop.is_closed() or self._shutdown.is_set():
            return

        self._shutdown.set()

        try:
            self.loop.run_until_complete(asyncio.sleep(1))
        except:
            try:
                self._shutdown.clear()
            except:
                pass
        finally:
            self.disconnect()

    def disconnect(self):
        try:
            for iapi in self.apis.values():
                iapi.close()
        except:
            pass

    def connect(self):
        try:
            istreamapi = api.APIClientASYNC(uri=cfg['ISTREAMAPI']['BASE_API'], loop=self.loop)
        except Exception as e:
            Logger.logger.error(f'Unable to initialize Ionostream API Client: {e}')
            exit(0)

        try:
            ttideapi = api.APIClientASYNC(uri=cfg['TECHTIDEAPI']['BASE_API'], loop=self.loop)
        except Exception as e:
            Logger.logger.error(f'Unable to initialize TechTIDE API Client: {e}')
            exit(0)

        self.apis['istreamapi'] = istreamapi
        self.apis['ttideapi'] = ttideapi

        self._connected = True

    def __init__(self):
        super().__init__()

        self._shutdown = asyncio.Event()
        self._connected = False
        self.apis = dict()
        self.connect()

    def __del__(self):
        try:
            self.stop()
        except:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()

class ISAOConn(IApiConn):
    def export(self, data, exppath, columns=None, overwrite=False):
        if exppath.exists():
            exppath.unlink(missing_ok=True)
        data.to_csv(exppath, sep=' ', columns=columns, na_rep='None', header=True, index=False, mode='w')

    async def querySAO_(self, verbose=False):
        from ionoapi import criono
        from ionoapi import api

        _ICHARS = ['foF2', 'mufD', 'fminF', 'qf', 'qe', 'phF2lyr', 'foF2p', 'b0IRI', 'b1IRI']
        try:
            # Gather REMOTE Ionostream Datasets
            dfO = await criono.Iono(
                start=self.start, end=self.end, bounds='[]', stations=self.stations
            ).df_(
                self.apis['istreamapi'], characteristics=_ICHARS,
                order_attrs=['timestamp', 'station'], order_by=['asc', ]
            )
            if verbose:
                Logger.logger.info(f'Retrieved {dfO.shape[0]} remote records')
        except Exception as e:
            Logger.logger.error(f'Unable to retrieve remote Ionospheric characteristics query results: {e}')
            exit(0)

        assert isinstance(dfO, pd.DataFrame), AssertionError('Ionospheric characteristics query results is not a valid type')

        expfile = f'ionchar_{self.start.strftime("%Y%m%dT%H%M")}_{self.end.strftime("%Y%m%dT%H%M")}.csv'
        exppath_ = self.exppath.joinpath(expfile)

        try:
            self.export(dfO, exppath_)
            Logger.logger.info(f'Successfully exported Ionospheric characteristics query results --> {exppath_}')
        except Exception as e:
            Logger.logger.error(f'Unable to export Ionospheric characteristics query results: {e}')
            exit(0)


    def querySAO(self, verbose=False):
        self.loop.run_until_complete(self.querySAO_(verbose=verbose))

    def __init__(self, start: Optional[datetime]=None, end:Optional[datetime]=None, stations: List[str]=None,
        resolution: str | None = '5m', exppath: str | Path=None, bounds='[]', attributes: List[str] = None,
        restrict: Optional[timedelta] = None, order_attrs: List[str] = None, order_by: List[str] = None):

        _bmapper = {('lower', '['): 'inclusive', ('upper', ']'): 'inclusive', ('lower', '('): 'exclusive',
                    ('upper', ')'): 'exclusive'}
        try:
            bounds_ = _BARGS.match(bounds).groupdict()
            bounds_ = {k: _bmapper[(k, v)] for k, v in bounds_.items()}
        except:
            raise ValueError(f'Malformed bounds attribute: {bounds}')

        _RESP = re.compile(r'^(?P<freqmul>\d+)(?P<freq>\w+)$')
        try:
            resolution_ = _RESP.match(resolution).groupdict()
            resolution_['freqmul'] = int(resolution_['freqmul'])
        except:
            raise ValueError(f'Malformed resolution attribute: {resolution}')

        end = ciso8601.parse_datetime(end) if isinstance(end, str) else (
            end if end else datetime.now(UTC).replace(tzinfo=None))

        start = ciso8601.parse_datetime(start) if isinstance(start, str) else (
            start if start else end-timedelta(hours=2))


        self.resolution = pd.Timedelta(resolution)

        start = pd.Timestamp(start).floor(self.resolution)
        end = pd.Timestamp(end).ceil(self.resolution)

        start = start.to_pydatetime()
        end = end.to_pydatetime()

        if bounds_['lower'] == 'exclusive':
            start += timedelta(milliseconds=1)

        if bounds_['upper'] == 'exclusive':
            end = end - timedelta(milliseconds=1)

        if restrict is not None:
            try:
                assert end - start <= restrict, AssertionError(f'Cannot request datasets for intervals more than {restrict}')
            except AssertionError as e:
                Logger.logger.error(f'{e}')
                exit(0)

        self.exppath=exppath

        self.start = start
        self.end = end
        self.stations = stations
        self.restrict = restrict

        if order_attrs:
            order_by = ["asc"] * len(order_attrs) if not order_by else order_by * len(order_attrs) if len(
                order_by) == 1 else order_by
        self.order_attrs = order_attrs
        self.attributes = attributes
        self.order_by = order_by

        super().__init__()

class IGridsConn(IApiConn):
    XNCCoords = np.arange(-10, 40 + 1, 1)
    YNCCoords = np.arange(80, 34 + (-1), -1)

    XTADMCoords = np.arange(-10, 40 + 1, 1)
    YTADMCoords = np.arange(72, 30 + (-1), -1)

    @staticmethod
    def ascii2pd(ascii):
        data_ = re.sub(r'(?m)^(?:[\w#].*)?\n?', '', ascii)
        df = pd.read_fwf(StringIO(data_), infer_nrows=51, header=None)
        return df

    def export(self, data, exppath, verbose=False):
        if exppath.exists():
            exppath.unlink(missing_ok=True)

        json_ = json.dumps(data, indent=4, default=str)
        if verbose:
            print(json_)
        exppath.write_text(json_)

    async def hmf2ascii2xr(self, hmf2ascii, hmf2nav):
        hmf2df = IGridsConn.ascii2pd(hmf2ascii)

        hmf2xr = xr.DataArray(hmf2df.values, dims=("y", "x"),
            coords={"y": IGridsConn.YTADMCoords, "x": IGridsConn.XTADMCoords},
            attrs={
                'name': 'hmF2', "long_name": 'hmF2 grid', "units": 'Km', 'timestamp': hmf2nav['timestamp'].isoformat(),
                "description": 'hmf2 (TAD2D Algorithm)'
            }
        )
        return hmf2xr

    async def fof2ascii2xr(self, fof2ascii, fof2nav):
        fof2df = IGridsConn.ascii2pd(fof2ascii)

        fof2xr = xr.DataArray(fof2df.values, dims=("y", "x"),
            coords={"y": IGridsConn.YNCCoords, "x": IGridsConn.XNCCoords},
            attrs={
                'name': 'foF2', "long_name": 'foF2 grid', "units": 'MHz', 'timestamp': fof2nav['timestamp'].isoformat(),
                "description": 'foF2 (DIASNC Algorithm)'
            }
        )
        return fof2xr

    async def queryGrid_(self, verbose=False):
        from ionoapi import crttide
        from ionoapi import api

        qobj = crttide.TTIDE(
            timestamp=self.timestamp, lat=self.lat, lon=self.lon
        )
        try:
            fof2nav = await qobj.nav_(self.apis['ttideapi'], type='fof2')
            hmf2nav = await qobj.nav_(self.apis['ttideapi'], type='hmf2')
            assert (fof2nav['uuid'] and hmf2nav['uuid']), AssertionError(f'404 <NODATA> for requested timestamp: {self.timestamp}')
        except Exception as e:
            Logger.logger.error(f'Unable to retrieve Modelled Grid Metadata: {e}')
            exit(0)

        try:
            fof2ascii = await qobj.data_(self.apis['ttideapi'], fof2nav)
            hmf2ascii = await qobj.data_(self.apis['ttideapi'], hmf2nav)
            assert (fof2ascii is not None and hmf2ascii is not None), AssertionError(f'404 <NODATA> for requested timestamp: {self.timestamp}')
        except Exception as e:
            Logger.logger.error(f'Unable to retrieve Modelled Grid Datasets: {e}')
            exit(0)

        try:
            fof2xrds = await self.fof2ascii2xr(fof2ascii, fof2nav)
            hmf2xrds = await self.hmf2ascii2xr(hmf2ascii, hmf2nav)
            fof2ds_ = fof2xrds.sel(y=self.lat, x=self.lon, method='nearest')
            hmf2ds_ = hmf2xrds.sel(y=self.lat, x=self.lon, method='nearest')
            resp = dict(
                req_timestamp=self.timestamp.isoformat() if self.timestamp is not None else 'null',
                lat=self.lat, lon=self.lon,
                foF2=fof2ds_.attrs | {'data': float(fof2ds_.data)},
                hmF2=hmf2ds_.attrs | {'data': float(hmf2ds_.data)}
            )
        except Exception as e:
            Logger.logger.error(f'Unable to process Modelled Grid Datasets: {e}')
            exit(0)

        expfile = f'grid_{self.timestamp.strftime("%Y%m%dT%H%M") if self.timestamp else "LAST"}_{int(self.lat):02d}_{int(self.lon):02d}.json'
        exppath_ = self.exppath.joinpath(expfile)

        try:
            self.export(resp, exppath_, verbose=verbose)
            Logger.logger.info(f'Successfully exported Modelled Grid Datasets (foF2, hmF2) --> {exppath_}')
        except Exception as e:
            Logger.logger.error(f'Unable to export Modelled Grid Datasets (foF2, hmF2): {e}')
            exit(0)

    def queryGrid(self, verbose=False):
        self.loop.run_until_complete(self.queryGrid_(verbose=verbose))

    def __init__(self, timestamp: Optional[datetime] = None, lat: float = None, lon: float = None,
                 resolution: str | None = '5m', exppath: str | Path = None):

        _RESP = re.compile(r'^(?P<freqmul>\d+)(?P<freq>\w+)$')
        try:
            resolution_ = _RESP.match(resolution).groupdict()
            resolution_['freqmul'] = int(resolution_['freqmul'])
        except:
            raise ValueError(f'Malformed resolution attribute: {resolution}')

        timestamp = ciso8601.parse_datetime(timestamp) if isinstance(timestamp, str) else timestamp

        self.resolution = pd.Timedelta(resolution)

        self.exppath = exppath

        self.timestamp = timestamp
        self.lat = lat
        self.lon = lon

        super().__init__()

class Configuration(object):
    CFG = dict()

    @classmethod
    def normpath(cls, extpath, basepath=F):
        if basepath is None:
            return None
        elif os.path.isabs(extpath):
            return extpath
        elif os.path.isfile(basepath):
            return str(os.path.normpath(os.path.join(os.path.dirname(basepath), extpath)))
        else:
            return str(os.path.normpath(os.path.join(basepath, extpath)))

    @classmethod
    def parseCFG(cls):
        cls.CFG['DATA_PATH'] = cls.normpath(cls.CFG['DATA_PATH'], F)
        cls.CFG['ETC_PATH'] = cls.normpath(cls.CFG['ETC_PATH'], F)
        cls.CFG['LOG_PATH'] = cls.normpath(cls.CFG['LOG_PATH'], F)

        for rk in ('STATIONS',):
            for k, v in cls.CFG[rk].items():
                cls.CFG[rk][k] = cls.normpath(v, F)

        for api in (['TECHTIDEAPI', 'ISTREAMAPI']):
            cls.CFG[api]['BASE'] = furl(cls.CFG[api]['BASE'])
            cls.CFG[api]['BASE_API'] = furl(cls.CFG[api]['BASE_API'])

    def __init__(self):
        _CFG = self.normpath('./conf.yaml')
        with open(_CFG) as f:
            Configuration.CFG = yaml.load(f, Loader=yaml.Loader)

        self.parseCFG()


_cfobj = Configuration()
cfg = _cfobj.CFG


class Logger_:

    class __Logger:
        @property
        def logger(self):
            if not self._logger:
                self.setLogger()
            return self._logger

        def setLogger(self, ):
            # create logger
            logger = logging.getLogger(cfg['App'])
            logger.setLevel(getattr(logging, cfg['LOGGING'].get('STDOUT','DEBUG')))

            fh = None
            if self.path:
                # create file handler which log even debug messages
                fh = logging.handlers.TimedRotatingFileHandler(self.path, when='midnight', interval=1,
                                                               backupCount=52 * 5, encoding=None, delay=0)
                fh.setLevel(getattr(logging, cfg['LOGGING'].get('FILE','INFO')))

            # create RQ handler with a higher log level
            ch = logging.StreamHandler()
            ch.setLevel(getattr(logging, cfg['LOGGING'].get('STREAM','DEBUG')))

            # create formatter and add it to the handlers
            formatter = logging.Formatter(fmt='%(levelname)s: %(message)s - %(asctime)s', datefmt='%H:%M:%S')
            if self.path:
                fh.setFormatter(formatter)
                logger.addHandler(fh)

            ch.setFormatter(formatter)
            logger.addHandler(ch)
            self._logger = logger

        def __init__(self, logpath=None):
            self._logger = None
            self.path = logpath

        def __str__(self):
            return repr(self)

    instance = None

    def __init__(self, logpath=None):
        if (not Logger_.instance) or (logpath and not Logger_.instance.path):
            Logger_.instance = Logger_.__Logger(logpath=logpath)
        else:
            pass

    def __getattr__(self, name):
        return getattr(self.instance, name)


Logger = Logger_()
_parser = None


def create_parser():

    def _main(args):
        exppath = None
        try:
            exppath = Path(args.exppath) if args.exppath else cfg['DATA_PATH'].joinpath('exports')
            if not exppath.is_absolute():
                exppath = Path(Configuration.normpath(exppath))
            assert exppath.exists, FileNotFoundError(f'Export Path {args.exppath} not found')
        except Exception as e:
            _parser.error(str(e))

        return dict(exppath=exppath)

    def ionosoper(args):
        _mainargs = _main(args)

        start, end = None, None
        if args.interval:
            period = []
            try:
                for v in args.interval:
                    period.append(None if v.lower() == 'null' else ciso8601.parse_datetime(v))
            except Exception as e:
                _parser.error(f"Error while parsing arguments 'START:<ISO8601>', 'END:<ISO8601>' : {e}")

            start, end = period

        stations = sorted(set(cfg['ISTREAMAPI']['Enabled'])) if args.stations=='all' else args.stations

        with ISAOConn(start=start, end=end, stations=stations, exppath=_mainargs['exppath'], restrict=timedelta(days=10)) as iapi:
            iapi.querySAO(verbose=args.verbose)

    def igridoper(args):
        from ionoapi import stations

        _mainargs = _main(args)

        try:
            timestamp =  None if args.timestamp.lower() == 'null' else ciso8601.parse_datetime(args.timestamp)
        except Exception as e:
            _parser.error(f"Error while parsing argument 'TSTAMP:<ISO8601>' : {e}")

        if args.station:
            stations_ = {_.code:_ for _ in stations.Stations().stations}
            try:
                _station = stations_[args.station[0]]
                lat, lon = _station.lat, _station.lon
            except Exception as e:
                _parser.error(f"Error while converting --station argument to coordinates : {e}")
        else:
            lat, lon = args.coordinates

        try:
            assert -10<=lon<=40 and 34<=lat<=72, AssertionError(f'Ensure LAT:{lat}, LON:{lon}, are in-bounds -> LAT:{{34N - 72N}}, LON:{{10W - 40E}}')
        except Exception as e:
            _parser.error(str(e))

        with IGridsConn(timestamp=timestamp, lat=lat, lon=lon, exppath=_mainargs['exppath']) as igapi:
            igapi.queryGrid(verbose=args.verbose)

    _parser = argparse.ArgumentParser(prog='IONOAPI_oper', description='IONOAPI Operations')
    _parser.add_argument('--version', action='version', version='1.0.0')
    _parser.add_argument("--exppath", type=Path, default=Path(cfg['DATA_PATH']).joinpath('exports'),
                         help="Data export path", required=False)
    _parser.add_argument('-v', '--verbose', action='store_true', help='Verbose mode')

    subparsers = _parser.add_subparsers(help='sub-command help')

    # ------- IonoChar Datasets parser -------
    ionchar_parser = subparsers.add_parser('ionos', help='Ionospheric Characteristics operations [tabular {CSV} format]')
    ionchar_parser.set_defaults(func=ionosoper)

    ionchar_parser.add_argument('-i', '--interval', nargs=2, type=str,
        action='store', metavar=('START:<ISO8601>', 'END:<ISO8601>'),
        help=f"<Required> Set period %(metavar)s, "
             f"DEFAULT: ('START':<null> | 'END':<null>), Max TDelta: 10days, END:<null> == NOW, START:<null> == END - 2hours", required=True)
    ionchar_parser.add_argument('-s', '--stations', nargs='+', type=str, help='Set stations (default: %(default)s)', default='all',
                         choices=sorted(set(cfg['ISTREAMAPI']['Enabled'])) + ['all',], required=False)

    # ------- IonoGrid Datasets parser -------
    iongrid_parser = subparsers.add_parser('igrid', help='Modelled Grid Datasets operations [application/json {JSON} format]')
    iongrid_parser.set_defaults(func=igridoper)

    iongrid_parser.add_argument('-t', '--timestamp', type=str,
        metavar=('TSTAMP:<ISO8601>'),
        help=f"<Required> Set query timestamp %(metavar)s, "
             f"DEFAULT: ('TSTAMP':<null>)", required=True)

    iongridpos_parser = iongrid_parser.add_mutually_exclusive_group(required=True)

    iongridpos_parser.add_argument('-s', '--station', type=str,
        help='Set station', nargs=1, choices=sorted(set(cfg['ISTREAMAPI']['Enabled'])), required=False)

    iongridpos_parser.add_argument('-c', '--coordinates', nargs=2, type=float,
        action='store', metavar=('LAT:<float>{34N - 72N}', 'LON:<float>{10W - 40E}'),
        help=f"<Set coordinates %(metavar)s, ", required=False)

    return _parser


parser = create_parser()

# Example Call:
# (igrid last)  --> python iapi.py -v --exppath ./exports igrid -t null -c 38 29
# (igrid @)     --> python iapi.py -v --exppath ./exports igrid -t 2025-02-03T12:35:00 -c 45 18
# (ionos last)  --> python iapi.py -v --exppath ./exports ionos -i null null
# Help: python iapi.py --help
def main(argv):
    Logger.logger.info('Remote IONOAPI Operations')

    args = parser.parse_args()
    if not bool(args.__dict__):
        raise KeyError('No arguments supplied')
    try:
        args.func(args)
    except Exception as e:
        raise RuntimeError(e)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
