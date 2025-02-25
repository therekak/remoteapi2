import json
import uuid as uuid_mod
import pyproj
from datetime import datetime
from typing import Optional, Literal, List, Union, Any
from pydantic import BaseModel, validator, Extra, Field, model_validator, BeforeValidator, PlainSerializer, field_validator, field_serializer
import geojson
from shapely.ops import transform
from shapely.geometry import Point, mapping,shape as sshape
from geoalchemy2 import shape
from geoalchemy2.elements import WKTElement, WKBElement
from ._fields import Hex,Bool

from iapi import Logger, cfg

APPUUID = uuid_mod.UUID(cfg['UUID'])
_PROJ4326_3857 = pyproj.Transformer.from_crs(pyproj.CRS('EPSG:4326'), pyproj.CRS('EPSG:3857'), always_xy=True).transform

def wktwkb_serializer(v, srid=None):
    if isinstance(v, str):
        return v

    shpg = shape.to_shape(v)
    geoj = mapping(shpg)
    if srid:
        geoj['srid'] = str(srid)
    ftr = geoj
    return json.dumps(ftr)

class StationSerial(BaseModel):
    id: Optional[uuid_mod.UUID] = None
    lat: float
    lon: float
    code: str
    type: str
    country: Optional[str] = None
    geom: Optional[Union[WKTElement, WKBElement]] = None
    geommerc: Optional[Union[WKTElement, WKBElement]] = None

    @model_validator(mode='before')
    def rootVLD(cls, values):
        uuid = uuid_mod.uuid5(APPUUID, str(values['code']))
        values['id'] = uuid
        return values

    @field_validator('lon', mode='before') # pre=True, always=True
    @classmethod
    def lonVLD(cls, v):
        v = float(v)
        return round(v-360.0 if v>180.0 else v,12)

    @field_validator('lat', mode='before') # ,pre=True, always=True
    @classmethod
    def latVLD(cls, v):
        return round(float(v), 12)

    @field_validator('geom', mode='before') #, pre=True, always=True
    @classmethod
    def geometryVLD(cls, v, values) -> Union[WKTElement, WKBElement]:
        if isinstance(v, WKBElement):
            return v
        elif isinstance(v, str):
            geom = sshape(geojson.loads(v))
        elif isinstance(v, dict):
            geom = Point(v['lon'], v['lat'])
        elif v is None:
            geom = Point(values.data['lon'], values.data['lat'])
        else:
            geom = v
        return WKTElement(geom.wkt, 4326)

    @field_serializer('geom')
    def geometrySRL(self, geom):
        return wktwkb_serializer(geom, 4326)


    @field_validator('geommerc', mode='before')
    @classmethod
    def geometrymVLD(cls, v, values) -> Union[WKTElement, WKBElement]:
        if isinstance(v, WKBElement):
            return v
        elif isinstance(v, str):
            geom_ = sshape(geojson.loads(v))
        elif isinstance(v, dict):
            geom = Point(v['lon'], v['lat'])
            geom_ = transform(_PROJ4326_3857, geom)
        elif v is None:
            geom = Point(values.data['lon'], values.data['lat'])
            geom_ = transform(_PROJ4326_3857, geom)
        else:
            geom_ = v

        return WKTElement(geom_.wkt, 3857)

    @field_serializer('geommerc')
    def geometrymSRL(self, geom):
        return wktwkb_serializer(geom, 3857)

    def toDB(self):
        raise NotImplementedError('IonoAPI is not allowed to update DB')
        #return md.IonoStation(self.dict())

    def todict(self):
        return self.model_dump()

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class StationDBO(StationSerial):

    geom: Union[str, Any]
    geommerc: Union[str, Any]

    @field_validator('geom', mode='before') # , pre=True, always=True
    @classmethod
    def geometryDBOVLD(cls, v):
        if isinstance(v, (WKBElement, WKTElement)):
            return wktwkb_serializer(v, srid=4326)
        elif isinstance(v, dict):
            return json.dumps(v)
        return v

    @field_validator('geommerc', mode='before') # , pre=True, always=True
    @classmethod
    def geometryMercDBOVLD(cls, v):
        if isinstance(v, (WKBElement, WKTElement)):
            return wktwkb_serializer(v, srid=3857)
        elif isinstance(v, dict):
            return json.dumps(v)
        return v

class GeophysicalConst(BaseModel):
    gyrofrequency: float
    dipAngle: float
    lat: float
    lon: float
    ssn: Optional[float]=None

    def __init__(self,data):
        super().__init__(**{k:v for k,v in zip(self.__fields__.keys(),data)})

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class System(BaseModel):
    sounder: str
    stationid: str
    ursicode: Optional[str] = None
    name: Optional[str] = Field(None, alias='NAME')
    artist: str = Field(None, alias='ARTIST')
    nhVer: Optional[str] = Field(None, alias='NH')
    adepVer: Optional[str] = Field(None, alias='ADEP')
    operMsg: Optional[str] = Field(None, alias='opermsg')

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class SoundingDPS(BaseModel):
    version: str
    timestamp: datetime
    rcvstation: str
    transtation: str
    dpsSched: Hex
    dpsProg: Hex
    startFreq: int
    coarseFreq: int
    stopFreq: int
    dpsFineFreqStep: int
    multiplexingDSBL: Bool
    ndpsSmallSteps: Hex
    dpsPhaseCode: Hex
    altANT1Setup: int
    dpsANT1Opts: Hex
    totalFFTSamplesPOW: int
    dpsRadioSilentMode: int
    pulseRepRate: int
    rangeStart: int
    dpsRangeIncr: str
    numRages: int
    scanDelay: int
    dpsBaseGain: Hex
    dpsFreqSearchEnabled: Bool
    dpsOpMode: int
    artistEnabled: Bool
    dpsDataFmt: int
    onlinePrinterSel: int
    ionoThreshFTP: int
    highInterference: int

    def __init__(self,data):
        data = [data[0],f'{data[1]}-{data[3]}-{data[4]}T{data[5]}:{data[6]}:{data[7]}'] + data[8:]
        super().__init__(**{k:v for k,v in zip(self.__fields__.keys(),data)})

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class SoundingDIGI256(BaseModel):
    version: str
    timestamp: datetime
    programSet: int
    programType: str
    journal: str
    nominalFreq: int
    outputCtrl: str
    startFreq: int
    incrFreq: Hex
    stopFreq: int
    testOutput: str
    stationid: str
    phaseCode: Hex
    ant1Azimuth: Hex
    ant1Scan: Hex
    ant1OptionDoppler: Hex
    numSamples: int
    repRate: Hex
    pulseWidthCode: Hex
    timeCtrl: Hex
    freqCorrection: Hex
    gainCorrection: Hex
    rangeIncr: Hex
    rangeStart: Hex
    freqSearch: Hex
    nominalGain: Hex
    spare: int

    def __init__(self,data):
        data = [data[0],f'{data[1]}-{data[3]}-{data[4]}T{data[5]}:{data[6]}:{data[7]}'] + data[9:]
        super().__init__(**{k:v for k,v in zip(self.__fields__.keys(),data)})

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class SoundingAISINGV(BaseModel):
    version: str
    timestamp: datetime

    def __init__(self,data):
        data = [data[0],f'{data[1]}-{data[3]}-{data[4]}T{data[5]}:{data[6]}:{data[7]}']
        super().__init__(**{k:v for k,v in zip(self.__fields__.keys(),data)})

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


def Sounding(data):
    parser = SoundingDPS if data[0]=='FF' else SoundingDIGI256 if data[0]=='FE' else SoundingAISINGV if data[0]=='AA' else None
    return parser(data)


class ModelExcludeUnset(BaseModel):

  def dict(self, *args, **kwargs):
      kwargs["exclude_unset"] = True
      return BaseModel.dict(self, *args, **kwargs)


class _ScaledIono(ModelExcludeUnset):
    foF2: Optional[float] = None
    foF1: Optional[float] = None
    mD: Optional[float] = None
    mufD: Optional[float] = None
    fmin: Optional[float] = None
    foEs: Optional[float] = None
    fminF: Optional[float] = None
    fminE: Optional[float] = None
    foE: Optional[float] = None
    fxI: Optional[float] = None
    hF: Optional[float] = None
    hF2: Optional[float] = None
    hE: Optional[float] = None
    hEs: Optional[float] = None
    zmE: Optional[float] = None
    yE: Optional[float] = None
    qf: Optional[float] = None
    qe: Optional[float] = None
    downF: Optional[float] = None
    downE: Optional[float] = None
    downEs: Optional[float] = None
    ff: Optional[float] = None
    fe: Optional[float] = None
    d: Optional[float] = None
    fMUF: Optional[float] = None
    hfMUF: Optional[float] = None
    delta_foF2: Optional[float] = None
    foEp: Optional[float] = None
    fhF: Optional[float] = None
    fhF2: Optional[float] = None
    foF1p: Optional[float] = None
    phF2lyr: Optional[float] = None
    phF1lyr: Optional[float] = None
    zhalfNm: Optional[float] = None
    foF2p: Optional[float] = None
    fminEs: Optional[float] = None
    yF2: Optional[float] = None
    yF1: Optional[float] = None
    tec: Optional[float] = None
    scHgtF2pk: Optional[float] = None
    b0IRI: Optional[float] = None
    b1IRI: Optional[float] = None
    d1IRI: Optional[float] = None
    foEa: Optional[float] = None
    hEa: Optional[float] = None
    foP: Optional[float] = None
    hP: Optional[float] = None
    fbEs: Optional[float] = None
    typeEs: Optional[float] = None

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore
        exclude_unset = True


class ScaledIono(_ScaledIono):

    def __init__(self, data):
        super().__init__(**{k:v for k,v in zip(self.__fields__.keys(),data)})

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class VTHADFGroup(BaseModel):
    virtualHeight: List[float]= Field(None, alias='VH')
    trueHeight: Optional[List[float]] = Field(None, alias='TH')
    amplitude: Optional[List[int]] = Field(None, alias='AMPL')
    dopplerNumber: Optional[List[int]] = Field(None, alias='DN')
    frequency: List[float] = Field(None, alias='FREQ')

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class VHADFGroup(BaseModel):
    virtualHeight: List[float]= Field(None, alias='VH')
    amplitude: Optional[List[int]] = Field(None, alias='AMPL')
    dopplerNumber: Optional[List[int]] = Field(None, alias='DN')
    frequency: List[float]= Field(None, alias='FREQ')

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class TFEGroup(BaseModel):
    trueHeight: List[float]= Field(None, alias='TH')
    frequency: List[float] = Field(None, alias='FREQ')
    electronDensity: List[float] = Field(None, alias='ELDENS')

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.ignore


class SAO(BaseModel):
    geoConst: GeophysicalConst
    system: System
    sounding: Union[SoundingDPS,SoundingDIGI256, SoundingAISINGV]
    scaled: ScaledIono
    analysisFlags: Optional[List[int]] = None
    dopplerTrans: Optional[List[float]] = None
    f2layerO: Optional[VTHADFGroup] = None
    f1layerO: Optional[VTHADFGroup] = None
    elayerO: Optional[VTHADFGroup] = None
    f2layerX: Optional[VHADFGroup] = None
    f1layerX: Optional[VHADFGroup] = None
    elayerX: Optional[VHADFGroup] = None
    medAmplF: Optional[List[int]] = None
    medAmplE: Optional[List[int]] = None
    medAmplEs: Optional[List[int]] = None
    trueHeightsCoefF2: Optional[List[float]] = None
    trueHeightsCoefF1: Optional[List[float]] = None
    trueHeightsCoefE: Optional[List[float]] = None
    quasiParabSegm: Optional[List[float]] = None
    editFlagsChar: Optional[List[int]] = None
    valleyDescrWDUM: Optional[List[float]] = None
    eslayerO: Optional[VHADFGroup] = None
    eauroralayerO: Optional[VHADFGroup] = None
    trueheightProf: Optional[TFEGroup] = None
    qualifLTR: Optional[List[str]] = None
    descrLTR: Optional[List[str]] = None
    editFlgTraceProf: Optional[List[int]] = None


    def hasTrueHeightProfile(self):
        if self.trueheightProf is not None and \
                self.trueheightProf.trueHeight is not None and \
                self.trueheightProf.electronDensity is not None:
            return True

        return False

    def toDB(self, id=None): #, stations
        raise NotImplementedError('IonoAPI is not allowed to update DB')

    def todict(self):
        return self.model_dump()




