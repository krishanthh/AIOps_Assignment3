# DA3408 Assignment 3 — Spark vs Ray

Krishanthh Mohan · DA24B044

Spark and Ray Data implement taxi-data ingestion, null filtering, global deduplication, pickup/dropoff zone joins, duration and average-speed features, and Parquet export. Both native and Python feature modes are provided.

## Submission contents and results

- [Report.pdf](Report.pdf): five-page benchmark report, graphs, winner and limitations.
- [benchmark_results.csv](benchmark_results.csv): original 36-month, approximately 1.9 GB measurements.
- `spark_clean.py`, `ray_clean.py`: corrected implementations. The two files in `legacy/` back the original full-size measurements; their original cluster configurations are under `docker/spark/` and `docker/ray/`.
- `Screenshots/`: cluster, execution and resource evidence supporting the reported measurements.
- `reports/`: current measured results, data provenance, machine description, raw resource samples and exact-parity outcomes.
- `scripts/`: only input preparation, benchmark orchestration and isolated feature timing.

Fresh verification uses **100,000 real TLC rows**, selected from two monthly files; it is separate from the original full-size benchmark. All four current outputs contain **99,994 matching complete rows**. The separate cross-file duplicate/invalid-row fixture retains four matching rows in every mode. See [exact parity](reports/current-parity.json), [edge-case results](reports/current-regression.json) and [current timings](reports/current-benchmarks.json).

The current Ray pipeline deduplicates across files; the historical monthly version did not guarantee cross-month parity. The validator compares complete row multisets and every output column, normalizing integer widths, timestamp precision and null/NaN representation without rounding finite floats. Feature timing uses materialized post-join data, one warm-up and three interleaved repetitions; it includes scheduling and a forced reduction, but excludes ingestion, cleaning, joins and export.

The proof video is outside this folder: `../assignment videos/Assignment_3_Spark_vs_Ray_Demo.mp4`. Its live 50,000-row Ray replay is labelled separately from the measured two-file verification.

## Setup

Use Linux, Docker/Compose, `uv`, and sufficient memory/disk for the input and generated outputs. Current execution reused the installed `da3408-node:latest` runtime: Spark 4.2.0, Ray 2.59.0 and Python 3.14.7. The replacement image build below is provided for a fresh machine and was not rerun during verification.

```bash
docker build -t da3408-node:latest -f docker/Dockerfile.proof .
uv venv --python 3.11 .venv
uv pip install --python .venv/bin/python -r requirements-host.txt
source .venv/bin/activate
```

The host environment is only for orchestration and full-row validation. Processing uses the container runtime. Generated data, outputs, logs and environments are intentionally excluded from the submission.

## Prepare and reproduce current measurements

```bash
docker run --rm --user "$(id -u):$(id -g)" --memory 384m --cpus 1 \
  -v "$PWD:/workspace" -w /workspace da3408-node:latest python scripts/prepare_proof.py

docker compose -f compose.proof.yml --profile spark up -d
python scripts/run_proof.py --framework spark
docker compose -f compose.proof.yml --profile spark stop

docker compose -f compose.proof.yml --profile ray up -d
python scripts/run_proof.py --framework ray
python parity_check_pyarrow.py
python verify_exact.py --output reports/current-regression.json \
  output/regression/spark_native output/regression/spark_udf \
  output/regression/ray_native output/regression/ray_udf
docker compose -f compose.proof.yml --profile ray stop
```

Input preparation downloads official January and February 2024 NYC TLC Yellow Taxi files, records their SHA256 values and selects the first 50,000 rows from each. The small correctness fixture is separate from all performance inputs. [Input provenance](reports/current-input.json) contains URLs, checksums and row counts.

Only run one framework at a time on a small machine. Each uses a coordinator and two one-CPU workers. Container memory caps are in `compose.proof.yml`; Spark workers advertise 512 MiB each, and Ray workers reserve 256 MiB task memory plus 96 MiB object storage. The Ray head has a larger cap for its driver and dashboard.

The runner recreates `logs/` and generated results. Omit `--resume` for fresh measurements; it is for recovering an interrupted run with its matching logs and outputs still present. The original full-size corrected-code benchmark was not repeated on this machine. For the historical workload, use the archived processing sources with the original configurations and 36 monthly files from 2022–2024, available from the [NYC TLC data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Live interfaces

- Spark Master: http://localhost:8080 — two ALIVE workers.
- Spark application: http://localhost:4040 while a job is active. Optional `--hold-ui-seconds 40` keeps it open after export; this wait is excluded from ETL timing.
- Ray Dashboard: http://localhost:8265 — head and two workers.

The report distinguishes historical and current resource allocations. Optional development helpers and raw console logs are archived outside the submission folders under `../assignment support/`; the required source, benchmark evidence and final report remain here.
