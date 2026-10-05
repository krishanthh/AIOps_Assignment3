"""Full-row parity checker; legacy aggregate-only checker is archived in legacy/."""
from pathlib import Path
from verify_exact import compare
root=Path(__file__).resolve().parent
scope="proof" if (root/"output/proof/spark_native").exists() else "full"
paths=[root/"output/proof"/name if scope=="proof" else root/"output"/(name+"_full") for name in ["spark_native","spark_udf","ray_native","ray_udf"]]
compare(paths,root/"reports"/("current-parity.json" if scope=="proof" else "full-parity.json"))
