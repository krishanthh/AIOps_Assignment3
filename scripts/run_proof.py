"""Execute real bounded verification; retain raw logs, measurements and exact comparisons."""
import argparse
import json
import os
import platform
import re
import subprocess
import threading
import time
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from verify_exact import compare

parser=argparse.ArgumentParser();parser.add_argument('--framework',choices=['spark','ray'],required=True);parser.add_argument('--resume',action='store_true');args=parser.parse_args()
framework=args.framework
reports=ROOT/'reports'; logs=ROOT/'logs'
reports.mkdir(exist_ok=True); logs.mkdir(exist_ok=True)
containers=[f'a3-proof-{framework}-{suffix}' for suffix in (['master','worker-1','worker-2'] if framework=='spark' else ['head','worker-1','worker-2'])]
SPARK='/usr/local/lib/python3.14/site-packages/pyspark/bin/spark-submit'
def command(script,arguments):
    if framework=='spark':
        return ['docker','exec','-e','PYSPARK_PYTHON=/usr/local/bin/python',containers[0],SPARK,'--master','spark://spark-master:7077','--driver-memory','512m','--executor-memory','480m','--executor-cores','1','--conf','spark.cores.max=2','--conf','spark.executor.memoryOverhead=64m','--conf','spark.sql.adaptive.enabled=false','/workspace/'+script,*arguments]
    return ['docker','exec',containers[0],'python','/workspace/'+script,*arguments]

def execute(name,cmd):
    saved=reports/(name+'.json')
    if args.resume and saved.exists() and (logs/(name+'.log')).exists():
        result=json.loads(saved.read_text())
        if result.get('exit_code')==0:
            print('RETAIN completed measurement',name,flush=True)
            return result
    print('START',name,flush=True)
    stop=threading.Event(); readings=[]
    def monitor():
        while not stop.is_set():
            result=subprocess.run(['docker','stats','--no-stream','--format','{{json .}}',*containers],capture_output=True,text=True)
            for line in result.stdout.splitlines():
                try: readings.append({'time':time.time(),**json.loads(line)})
                except ValueError: pass
            stop.wait(1)
    thread=threading.Thread(target=monitor,daemon=True);thread.start()
    started=time.time()
    with (logs/(name+'.log')).open('w') as stream:
        process=subprocess.run(cmd,stdout=stream,stderr=subprocess.STDOUT)
    stop.set();thread.join(timeout=5)
    (reports/(name+'-resources.json')).write_text(json.dumps(readings,indent=2))
    text=(logs/(name+'.log')).read_text()
    if process.returncode:
        print(text[-7000:],flush=True)
        raise RuntimeError(f'{name}: exit {process.returncode}')
    record={'name':name,'exit_code':0,'started_at':started,'finished_at':time.time(),'log':'logs/'+name+'.log'}
    for prefix in ['BENCHMARK_JSON ','FEATURE_JSON ']:
        for line in text.splitlines():
            if line.startswith(prefix): record.update(json.loads(line[len(prefix):]))
    if framework=='spark' and 'total_time_seconds' not in record and not name.endswith('feature'):
        # The original Spark script prints its JSON over several lines.
        position=text.rfind('\n{')
        if position>=0:
            decoder=json.JSONDecoder()
            try: record.update(decoder.raw_decode(text[position+1:])[0])
            except ValueError: pass
    (reports/(name+'.json')).write_text(json.dumps(record,indent=2))
    print('COMPLETE',name,json.dumps({k:v for k,v in record.items() if k in ['total_time_seconds','median_seconds','rows']}),flush=True)
    return record

results=[]
for mode in ['native','udf']:
    results.append(execute(framework+'-'+mode,command(framework+'_clean.py',['--input','/workspace/data/proof/*.parquet','--zones','/workspace/data/taxi_zone_lookup.csv','--output',f'/workspace/output/proof/{framework}_{mode}','--mode',mode])))
for mode in ['native','udf']:
    execute(framework+'-regression-'+mode,command(framework+'_clean.py',['--input','/workspace/data/regression/*.parquet','--zones','/workspace/data/taxi_zone_lookup.csv','--output',f'/workspace/output/regression/{framework}_{mode}','--mode',mode]))
execute(framework+'-feature',command('scripts/feature_benchmark.py',['--framework',framework,'--input','/workspace/data/proof/*.parquet','--zones','/workspace/data/taxi_zone_lookup.csv']))
compare([ROOT/'output/proof'/f'{framework}_{mode}' for mode in ['native','udf']],reports/(framework+'-mode-parity.json'))
regression=compare([ROOT/'output/regression'/f'{framework}_{mode}' for mode in ['native','udf']],reports/(framework+'-regression-parity.json'))
assert all(r['rows']==4 for r in regression['results'])
host={'host_architecture':platform.machine(),'logical_cpus':os.cpu_count(),'physical_memory_bytes':os.sysconf('SC_PAGE_SIZE')*os.sysconf('SC_PHYS_PAGES'),'verification_scope':'100,000 real TLC input rows, separate synthetic edge-case regression fixture, two workers with one CPU each. Historical full-dataset timings are not replaced by these fresh verification numbers.'}
(reports/'current-machine.json').write_text(json.dumps(host,indent=2))
print('FRAMEWORK VERIFICATION PASSED',framework,flush=True)
