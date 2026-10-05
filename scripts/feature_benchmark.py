"""Measure only the average-speed operation on an already materialized base."""
import argparse
import glob
import json
import statistics
import time

parser=argparse.ArgumentParser();parser.add_argument('--framework',choices=['spark','ray'],required=True);parser.add_argument('--input',required=True);parser.add_argument('--zones',required=True);args=parser.parse_args()
samples={'native':[],'udf':[]}
if args.framework=='spark':
    from pyspark.sql import functions as f
    from spark_clean import build_spark,build_pipeline,average_speed_udf
    spark=build_spark()
    base=build_pipeline(spark,args.input,args.zones,'native').drop('average_speed_mph').cache()
    count=base.count()
    def execute(mode):
        value=f.when(f.col('trip_duration_hours')>0,f.col('trip_distance')/f.col('trip_duration_hours')) if mode=='native' else average_speed_udf(f.col('trip_distance'),f.col('trip_duration_hours'))
        return base.select(value.alias('speed')).agg(f.sum('speed')).collect()[0][0]
    for mode in samples: execute(mode)
    for _ in range(3):
        for mode in samples:
            start=time.perf_counter(); value=execute(mode);samples[mode].append(time.perf_counter()-start)
    spark.stop()
else:
    import ray
    from ray_clean import build_base,add_feature
    ray.init(address='auto')
    base=build_base(sorted(glob.glob(args.input)),args.zones).materialize()
    count=base.count()
    def execute(mode): return add_feature(base,mode).sum('average_speed_mph',ignore_nulls=True)
    for mode in samples: execute(mode)
    for _ in range(3):
        for mode in samples:
            start=time.perf_counter();value=execute(mode);samples[mode].append(time.perf_counter()-start)
    ray.shutdown()
result={'framework':args.framework,'rows':count,'scope':'Only average-speed calculation and its forced reduction on materialized post-join data; includes task scheduling and reduction, excludes ingestion, cleaning, joins and export. One untimed warm-up per mode; three interleaved timed repetitions.','seconds':samples,'median_seconds':{mode:statistics.median(v) for mode,v in samples.items()}}
print('FEATURE_JSON '+json.dumps(result),flush=True)
