#!/usr/bin/env bash
# Upload ../data (song_data/ and log_data/) to s3://<raw-bucket>/raw/
set -euo pipefail

cd "$(dirname "$0")/.."
DATA_DIR="${DATA_DIR:-../data}"
RAW_BUCKET="$(terraform -chdir=infra output -raw raw_bucket)"

aws s3 sync "$DATA_DIR/song_data" "s3://$RAW_BUCKET/raw/song_data" --only-show-errors
aws s3 sync "$DATA_DIR/log_data"  "s3://$RAW_BUCKET/raw/log_data"  --only-show-errors
echo "Uploaded $DATA_DIR to s3://$RAW_BUCKET/raw/"
