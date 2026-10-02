# 04 · Data lake with Spark: S3 raw JSON → clean, partitioned Parquet

**What you'll learn:** reading messy JSON with explicit schemas, quarantining bad
records, building dimensional tables with the Spark DataFrame API, writing
partitioned Parquet without a tiny-files problem, and running the same job locally
and on **Amazon EMR Serverless**, then querying the result with **Athena**.

## Why a data lake?

A warehouse (project 03) loads data into its own storage first. A data lake keeps
data as files in cheap object storage (S3) in an open, columnar format (Parquet),
and lets *many* engines read it: Spark, Athena, Redshift Spectrum, DuckDB, pandas.
The typical layout is zones:

```
s3://<raw-bucket>/raw/        ← data exactly as it arrived (JSON), never modified
s3://<lake-bucket>/curated/   ← cleaned, deduplicated, modeled, Parquet
```

## What the job does (`etl.py`)

```
raw/song_data/**.json ─┐                         ┌─► curated/songs/year=…/
                       ├─► clean + model (Spark) ├─► curated/artists/
raw/log_data/**.json ──┘          │              ├─► curated/users/
                                  │              ├─► curated/time/year=…/month=…/
                                  │              └─► curated/songplays/year=…/month=…/
                                  └─► curated/_quarantine/log_data/   (malformed lines)
```

Things worth reading in the code:

- **Explicit schemas** (`SONG_SCHEMA`, `LOG_SCHEMA`): no inference pass, and a bad
  file can't silently change a column type.
- **Quarantine instead of crash**: `mode=PERMISSIVE` + `_corrupt_record` sends
  malformed lines to `_quarantine/` and keeps the job going.
- **Window functions for dedupe**: latest level per user, best row per artist.
- **Deterministic ids**: `songplay_id = sha2(user_id|session_id|ts)` stays the same
  on every re-run, unlike `monotonically_increasing_id()`.
- **Broadcast join** of the small catalog onto the large play table.
- **Partitioning with intent**: `songplays` and `time` by `year/month` (queries
  filter by date). `songs` by `year` only. Partitioning songs by `artist_id`, as
  many tutorials do, creates thousands of tiny files, which slows every reader.
- **Dynamic partition overwrite**: re-running only replaces the partitions present
  in the new output, so the job is idempotent.
- **Data quality checks** (row counts, unique and non-null keys) fail the job before
  anything is written.

The same 3,802 plays / 2,756 catalog matches come out here as in projects 01 and
03. That consistency is itself a useful test.

## Run it locally

Needs Python 3.11+ and Java 17 or 21 (`java -version`).

```bash
python shared/generate_sample_data.py          # from the repo root, once

cd 04-spark-data-lake
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python etl.py --input ../data --output ./output
pytest tests                                   # unit tests on a local SparkSession
```

Have a look at the result:

```python
import pandas as pd
pd.read_parquet("output/songplays").groupby("level").size()
```

Want to see the quarantine in action? Append a broken line to any log file
(`echo '{"page": "NextSong", broken' >> ../data/log_data/2026/09/2026-09-02-events.json`)
and run again.

## Run it on AWS (EMR Serverless + Glue + Athena)

You need AWS credentials, the AWS CLI v2 and Terraform ≥ 1.6.

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # optional
terraform init && terraform apply              # buckets, IAM roles, EMR app, Glue, Athena
cd ..

scripts/upload_raw.sh                          # ../data -> s3://<raw>/raw/
scripts/run_emr_serverless.sh                  # submit, wait, then start the Glue crawler
```

The Terraform creates:

| Resource | Why |
|---|---|
| `raw` and `lake` S3 buckets | private, encrypted; job logs and Athena results expire automatically |
| EMR Serverless **application** (`emr-7.14.0`, Spark 3.5) | capped at 8 vCPU / 32 GB, auto-stops after 5 idle minutes |
| **job role** | the job can read `raw`, read/write `lake`, nothing else |
| Glue database + **crawler** | registers the five Parquet tables and their partitions |
| Athena **workgroup** | query results go to `s3://<lake>/athena-results/` |

Once the crawler finishes (~1–2 minutes), open Athena, choose the
`sparkify-lake` workgroup and the `sparkify_lake_curated` database:

```sql
SELECT s.title, COUNT(*) AS plays
FROM songplays sp JOIN songs s ON s.song_id = sp.song_id
WHERE sp.year = 2026 AND sp.month = 9        -- partition pruning: only scans September
GROUP BY s.title ORDER BY plays DESC LIMIT 10;
```

You can also point the job at S3 from your laptop with
`python etl.py --input s3://<raw>/raw --output s3://<lake>/curated --local-s3`. It
downloads the `hadoop-aws` connector that matches your pyspark's Hadoop version.

**Clean up** with `cd infra && terraform destroy`. EMR Serverless and Athena bill
per use (vCPU-seconds / data scanned); nothing here costs money while idle except a
few MB of S3 storage.

## Things to try next

- Write the curated tables as **Apache Iceberg** instead of plain Parquet to get
  `MERGE`, time travel and schema evolution (EMR 7 supports it out of the box).
- Make the job incremental: process one day of `log_data` per run and pass the date
  as an argument (Airflow in project 05 would supply it).
- Load `curated/` into Redshift with `COPY … FORMAT AS PARQUET` and compare with project 03.
