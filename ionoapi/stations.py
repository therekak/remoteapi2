from iapi import cfg

from . import schemas

class Stations(object):

    def store(self):
        raise NotImplementedError

    def parse(self,path, stype = None):
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                args = {k:v for k,v in zip(['code','lat','lon'],[r.strip() for r in line.split()])}
                args['type'] = stype
                sstation = schemas.StationSerial(**args)
                self.stations.append(sstation)

    def __init__(self):
        cfgstations = cfg['STATIONS']
        self.stations = []

        for key,stype in zip(('EU_STATIONS','GLOBAL_STATIONS','GLOBAL_GNSS_STATIONS'),('iono','iono','gnss')):
            self.parse(cfgstations[key], stype = stype)