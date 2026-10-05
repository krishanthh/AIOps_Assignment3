"""Global Ray Data ETL. Historical monthly implementation is retained in legacy/."""
import argparse
import glob
import json
import time

import numpy as np
import pandas as pd
import ray
from ray.data.expressions import col

TRIP_COLUMNS = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance', 'PULocationID', 'DOLocationID', 'fare_amount']
DEDUP_COLUMNS = TRIP_COLUMNS
ZONE_COLUMNS = ['LocationID', 'Borough', 'Zone', 'service_zone']

def prepare_trips(batch):
    batch = batch.copy()
    for name in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
        batch[name] = pd.to_datetime(batch[name], errors='coerce')
    for name in ['trip_distance', 'fare_amount']:
        batch[name] = pd.to_numeric(batch[name], errors='coerce').astype('float64')
    for name in ['VendorID', 'PULocationID', 'DOLocationID']:
        batch[name] = pd.to_numeric(batch[name], errors='coerce').astype('Int64')
    batch['trip_duration_hours'] = (batch.tpep_dropoff_datetime - batch.tpep_pickup_datetime).dt.total_seconds() / 3600.0
    return batch

def one_per_key(batch):
    # map_groups receives the entire key group, including cross-file duplicates.
    return batch.iloc[:1]

def average_speed_udf(batch):
    batch = batch.copy()
    duration = batch['trip_duration_hours'].to_numpy(dtype=np.float64)
    distance = batch['trip_distance'].to_numpy(dtype=np.float64)
    result = np.full(len(batch), np.nan)
    np.divide(distance, duration, out=result, where=duration > 0)
    batch['average_speed_mph'] = result
    return batch

def build_base(input_files, zones_path, blocks=8):
    from pyarrow.parquet import ParquetFile
    input_rows = sum(ParquetFile(path).metadata.num_rows for path in input_files)
    # Avoid empty hash-join partitions for tiny correctness fixtures.
    blocks = min(blocks, max(1, input_rows // 1000))
    trips = ray.data.read_parquet(input_files, columns=TRIP_COLUMNS, override_num_blocks=blocks)
    required = ['tpep_pickup_datetime','tpep_dropoff_datetime','trip_distance','PULocationID','DOLocationID']
    expression = col(required[0]).is_not_null()
    for name in required[1:]:
        expression = expression & col(name).is_not_null()
    trips = trips.filter(expr=expression)
    trips = trips.groupby(DEDUP_COLUMNS, num_partitions=blocks).map_groups(one_per_key, batch_format='pandas')
    trips = trips.map_batches(prepare_trips, batch_format='pandas')
    zones = pd.read_csv(zones_path, keep_default_na=False, na_values=[''])[ZONE_COLUMNS]
    zones['LocationID'] = zones['LocationID'].astype('int64')
    zones = ray.data.from_pandas(zones)
    pickup = zones.rename_columns({'LocationID':'PULocationID','Borough':'pickup_borough','Zone':'pickup_zone','service_zone':'pickup_service_zone'})
    dropoff = zones.rename_columns({'LocationID':'DOLocationID','Borough':'dropoff_borough','Zone':'dropoff_zone','service_zone':'dropoff_service_zone'})
    trips = trips.join(pickup, join_type='inner', num_partitions=blocks, on=('PULocationID',))
    return trips.join(dropoff, join_type='inner', num_partitions=blocks, on=('DOLocationID',))

def add_feature(dataset, mode):
    if mode == 'udf':
        return dataset.map_batches(average_speed_udf, batch_format='pandas')
    duration = col('trip_duration_hours')
    positive = (duration.sign() + duration.sign().abs()) / 2
    return dataset.with_column('average_speed_mph', col('trip_distance') * positive / (duration * positive))

def run(args):
    files = sorted(glob.glob(args.input))
    if not files:
        raise FileNotFoundError(args.input)
    ray.init(address='auto')
    start = time.perf_counter()
    dataset = add_feature(build_base(files, args.zones, args.blocks), args.mode)
    dataset.write_parquet(args.output, mode='overwrite')
    result = {'framework':'ray','mode':args.mode,'total_time_seconds':round(time.perf_counter()-start,6),'num_input_files':len(files),'execution_strategy':'global_deduplication_across_all_input_files'}
    print('BENCHMARK_JSON ' + json.dumps(result), flush=True)
    ray.shutdown()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',required=True)
    parser.add_argument('--zones',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--mode',choices=['native','udf'],required=True)
    parser.add_argument('--blocks',type=int,default=8)
    run(parser.parse_args())
