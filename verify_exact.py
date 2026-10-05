"""Compare complete row multisets, including zone fields and exact finite floats."""
import argparse
import datetime
import json
import math
import sqlite3
from pathlib import Path
import pyarrow.dataset as ds

FIELDS=['VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','trip_distance','PULocationID','DOLocationID','fare_amount','pickup_borough','pickup_zone','pickup_service_zone','dropoff_borough','dropoff_zone','dropoff_service_zone','trip_duration_hours','average_speed_mph']
INTEGERS={'VendorID','PULocationID','DOLocationID'}
FLOATS={'trip_distance','fare_amount','trip_duration_hours','average_speed_mph'}
def key(row):
    result=[]
    for name in FIELDS:
        value=row[name]
        if value is None or isinstance(value,float) and math.isnan(value): value=None
        elif name in INTEGERS: value=int(value)
        elif name in FLOATS: value=float(value).hex()
        elif isinstance(value,datetime.datetime): value=value.isoformat(timespec='microseconds')
        result.append(value)
    return json.dumps(result,separators=(',',':'),ensure_ascii=False)

def compare(paths, output):
    database=Path(str(output)+'.sqlite')
    database.unlink(missing_ok=True)
    connection=sqlite3.connect(database)
    connection.execute('CREATE TABLE counts (row TEXT PRIMARY KEY, baseline INTEGER NOT NULL, delta INTEGER NOT NULL)')
    summaries=[]
    for index,path in enumerate(paths):
        dataset=ds.dataset(path,format='parquet')
        assert set(dataset.schema.names)==set(FIELDS), (path,dataset.schema.names)
        count=0
        if index: connection.execute('UPDATE counts SET delta=-baseline')
        for batch in dataset.scanner(columns=FIELDS,batch_size=8192).to_batches():
            rows=[(key(row),) for row in batch.to_pylist()]
            if index==0:
                connection.executemany('INSERT INTO counts VALUES (?,1,0) ON CONFLICT(row) DO UPDATE SET baseline=baseline+1',rows)
            else:
                connection.executemany('INSERT INTO counts VALUES (?,0,1) ON CONFLICT(row) DO UPDATE SET delta=delta+1',rows)
            count+=len(rows)
        mismatches=0 if index==0 else connection.execute('SELECT count(*) FROM counts WHERE delta != 0').fetchone()[0]
        summaries.append({'path':str(path),'rows':count,'different_full_rows':mismatches,'passed':mismatches==0})
        connection.commit()
    connection.close()
    database.unlink()
    result={'method':'Exact multiset comparison of every output column using canonical rows; integer widths and timestamp precision normalized; null/NaN speed represented identically; finite floats compared by their exact IEEE hex representation. No rounding or aggregate-only comparison.','results':summaries,'passed':all(s['passed'] for s in summaries)}
    Path(output).write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2),flush=True)
    if not result['passed']: raise SystemExit(1)
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);parser.add_argument('paths',nargs='+');args=parser.parse_args()
    compare(args.paths,args.output)
