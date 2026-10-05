"""A bounded real-data verification input; never substituted for historical 2 GB benchmarks."""
import hashlib
import json
import shutil
import urllib.request
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[1]
data=ROOT/'data'
(data/'proof').mkdir(parents=True,exist_ok=True)
(data/'regression').mkdir(exist_ok=True)
sources=[]
for month in ['2024-01','2024-02']:
    name=f'yellow_tripdata_{month}.parquet'
    path=data/name
    url=f'https://d37ci6vzurychx.cloudfront.net/trip-data/{name}'
    if not path.exists():
        print('Downloading',name,flush=True)
        urllib.request.urlretrieve(url,path)
    columns=['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','trip_distance','PULocationID','DOLocationID','fare_amount']
    file=pq.ParquetFile(path)
    batch=next(file.iter_batches(batch_size=50000,columns=columns))
    pq.write_table(pa.Table.from_batches([batch]),data/'proof'/name)
    digest=hashlib.sha256(path.read_bytes()).hexdigest()
    sources.append({'url':url,'sha256':digest,'downloaded_bytes':path.stat().st_size,'source_rows':file.metadata.num_rows,'verification_rows':batch.num_rows})
lookup=data/'taxi_zone_lookup.csv'
if not lookup.exists():
    urllib.request.urlretrieve('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',lookup)
# Intentional edge cases live separately from all performance inputs.
row={'VendorID':1,'tpep_pickup_datetime':pd.Timestamp('2024-01-01 10:00:00'),'tpep_dropoff_datetime':pd.Timestamp('2024-01-01 10:30:00'),'trip_distance':10.0,'PULocationID':1,'DOLocationID':2,'fare_amount':20.0}
zero={**row,'trip_distance':3.0,'tpep_dropoff_datetime':row['tpep_pickup_datetime']}
negative={**row,'trip_distance':4.0,'tpep_dropoff_datetime':pd.Timestamp('2024-01-01 09:00:00')}
valid={**row,'trip_distance':5.0}
invalid={**row,'trip_distance':None}
pd.DataFrame([row,zero]).to_parquet(data/'regression/part-1.parquet',index=False)
pd.DataFrame([row,negative,valid,invalid]).to_parquet(data/'regression/part-2.parquet',index=False)
manifest={'scope':'Fresh verification only: first 50,000 real rows from each of two 2024 TLC monthly files. Historical 36-month benchmark results are retained separately.','rows_before_cleaning':100000,'sources':sources,'regression_fixture':'Separate six-row fixture: one cross-file duplicate, one null-distance row, zero/negative durations. Expected retained rows: 4.'}
(ROOT/'reports/current-input.json').write_text(json.dumps(manifest,indent=2))
print(json.dumps(manifest,indent=2),flush=True)
