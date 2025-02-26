### Instructions on how to retrieve data (ionospheric characteristics)

------------------------------------------------------------------------------------------------------------------------

Run **python iapi.py --help** to get details on:  

- positional arguments:  
  - **ionos**	***Ionospheric Characteristics operations [tabular {CSV} format]***  
The ionospheric characteristics foF2, mufD, fminF, qf, qe, phF2lyr, foF2p, b0IRI, b1IRI for the European Digisonde stations AT138, DB049, EA036, EB040, JR055, PQ052, RL052, RO041, SO148, TR170 are provided in CSV format.  
  - **igrid**	***Modelled Grid Datasets operations [application/json {JSON} format]***  
The values of foF2 calculated by the DIASNC algorithm and hmF2 calculated by the TaD2D algorithm at a concrete location in the European grid (latitude: 34N - 72N, longitude: 10W - 40E) are provided in JSON format.  

- options:  
  - **--exppath** EXPATH Data export path  
  - **-v, --verbose** Verbose mode  

------------------------------------------------------------------------------------------------------------------------

Run **python iapi.py ionos --help** to get details on the query parameters: 

- **-i START:<ISO8601> END:<ISO8601>, --interval START:<ISO8601> END:<ISO8601>**  
Set temporal period in ISO8601 format (YYYY-MM-DDThh:mm:ss)  
Maximum interval (Max TDelta): 10 days  
Default values: 'START':<null> 'END':<null> --> The data for the last 2 hours will be returned  

- **-s {AT138,DB049,EA036,EB040,JR055,PQ052,RL052,RO041,SO148,TR170,all} [{AT138,DB049,EA036,EB040,JR055,PQ052,RL052,RO041,SO148,TR170,all} ...], --stations {AT138,DB049,EA036,EB040,JR055,PQ052,RL052,RO041,SO148,TR170,all} [{AT138,DB049,EA036,EB040,JR055,PQ052,RL052,RO041,SO148,TR170,all} ...]**  
Set station(s)  
Default value: all --> The data from all the stations will be returned  
Available stations: AT138 (Athens, Greece), DB049 (Dourbes, Belgium), EA036 (El Arenosillo, Spain), EB040 (Roquetes, Spain), JR055 (Juliusruh, Germany), PQ052 (Pruhonice, Czechia), RL052 (Reilich, UK), RO041 (Rome, Italy), SO148 (Sopron, Hungary), TR170 (Tromso, Norway)  
  
Example query:  
- Run **python iapi.py -v ionos -i 2024-02-10T00:00:00 2024-02-16T00:00:00 -s AT138 EB040 SO148** to get data from 2024-02-10T00:00:00 until 2024-02-16T00:00:00 for the AT138, EB040 and SO148 Digisonde stations  
- Response:  
  - INFO: Successfully exported Ionospheric characteristics query results --> EXPATH/ionchar_20250210T0000_20250216T0000.csv  
  - The results of the query are available in the **CSV file** ionchar_20250210T0000_20250216T0000.csv at the data export path (EXPATH).  

------------------------------------------------------------------------------------------------------------------------

Run **python iapi.py igrid --help** to get details on the query parameters:  

- **-t TSTAMP:<ISO8601>, --timestamp TSTAMP:<ISO8601>**  
Set timestamp in ISO8601 format (YYYY-MM-DDThh:mm:ss)  

- **-s {AT138,DB049,EA036,EB040,JR055,PQ052,RL052,RO041,SO148,TR170}, --station {AT138,DB049,EA036,EB040,JR055,PQ052,RL052,RO041,SO148,TR170}**  
Set station  

- **-c LAT:<float>{34N - 72N} LON:<float>{10W - 40E}, --coordinates LAT:<float>{34N - 72N} LON:<float>{10W - 40E}**  
Set coordinates for the European grid: LAT (latitude): from 34 to 72 (34N - 72N), LON (longitude): from -10 to 40 (10W - 40E)  
  
Example query 1:  
- Run **python iapi.py -v igrid t 2025-02-24T12:00:00 -c 54 29** to get data at 2025-02-24T12:00:00 at the requested location (lat:54, lon:29).  
- Response:  
  - {  
    "req_timestamp": "2025-02-24T12:00:00",  
    "lat": 54.0,  
    "lon": 29.0,  
    "foF2": {  
        "name": "foF2",  
        "long_name": "foF2 grid",  
        "units": "MHz",  
        "timestamp": "2025-02-24T11:00:00",  
        "description": "foF2 (DIASNC Algorithm)",  
        "data": 11.7333  
    },  
    "hmF2": {  
        "name": "hmF2",  
        "long_name": "hmF2 grid",  
        "units": "Km",  
        "timestamp": "2025-02-24T12:00:00",  
        "description": "hmf2 (TAD2D Algorithm)",  
        "data": 285.98  
    }  
}  
  - INFO: Successfully exported Modelled Grid Datasets (foF2, hmF2) --> /home/ionos/Projects/datastream/clients/remoteapi2/bin/exports/grid_20250224T1200_54_29.json  
  - The results of the query are available in the **JSON file** grid_20250224T1200_54_29.json at the data export path (EXPATH).  
  
Example query 2:  
- Run **python iapi.py -v igrid t 2025-02-24T12:00:00 -s DB049** to get data at 2025-02-24T12:00:00 at the requested location (Digisonde station DB049 location).  
- Response:  
  - {  
    "req_timestamp": "2025-02-24T12:00:00",  
    "lat": 50.1,  
    "lon": 4.6,  
    "foF2": {  
        "name": "foF2",  
        "long_name": "foF2 grid",  
        "units": "MHz",  
        "timestamp": "2025-02-24T11:00:00",  
        "description": "foF2 (DIASNC Algorithm)",  
        "data": 12.1  
    },  
    "hmF2": {  
        "name": "hmF2",  
        "long_name": "hmF2 grid",  
        "units": "Km",  
        "timestamp": "2025-02-24T12:00:00",  
        "description": "hmf2 (TAD2D Algorithm)",  
        "data": 274.78  
    }  
}  
  - INFO: Successfully exported Modelled Grid Datasets (foF2, hmF2) --> /home/ionos/Projects/datastream/clients/remoteapi2/bin/exports/grid_20250224T1200_50_04.json  
  - The results of the query are available in the **JSON file** grid_20250224T1200_50_04.json at the data export path (EXPATH).

