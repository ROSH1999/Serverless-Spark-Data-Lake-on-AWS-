#!/usr/bin/env bash
# Submit etl.py to EMR Serverless, wait for it, and (optionally) crawl the output.
#
#   scripts/run_emr_serverless.sh            # run job, then start the Glue crawler
#   CRAWL=0 scripts/run_emr_serverless.sh    # skip the crawler
set -euo pipefail

cd "$(dirname "$0")/.."
tf() { terraform -chdir=infra output -raw "$1"; }

REGION="$(tf region)"
APP_ID="$(tf emr_application_id)"
ROLE_ARN="$(tf emr_job_role_arn)"
RAW_BUCKET="$(tf raw_bucket)"
LAKE_BUCKET="$(tf lake_bucket)"
export AWS_REGION="$REGION"

# 1. Ship the job code. EMR Serverless reads the entry point from S3.
aws s3 cp etl.py "s3://$LAKE_BUCKET/code/etl.py" --only-show-errors

# 2. Start the job. Small executors are plenty for the sample data; scale these
#    up (and max_vcpu in Terraform) for real volumes.
JOB_DRIVER=$(cat <<JSON
{
  "sparkSubmit": {
    "entryPoint": "s3://$LAKE_BUCKET/code/etl.py",
    "entryPointArguments": ["--input", "s3://$RAW_BUCKET/raw", "--output", "s3://$LAKE_BUCKET/curated"],
    "sparkSubmitParameters": "--conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.dynamicAllocation.maxExecutors=4"
  }
}
JSON
)
OVERRIDES=$(cat <<JSON
{"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": "s3://$LAKE_BUCKET/logs/"}}}
JSON
)

JOB_RUN_ID=$(aws emr-serverless start-job-run \
  --application-id "$APP_ID" \
  --execution-role-arn "$ROLE_ARN" \
  --name "sparkify-lake-etl-$(date +%Y%m%d-%H%M%S)" \
  --job-driver "$JOB_DRIVER" \
  --configuration-overrides "$OVERRIDES" \
  --query jobRunId --output text)
echo "Started job run $JOB_RUN_ID (application $APP_ID)"

# 3. Poll until the job finishes.
while true; do
  STATE=$(aws emr-serverless get-job-run --application-id "$APP_ID" --job-run-id "$JOB_RUN_ID" \
    --query jobRun.state --output text)
  echo "  $(date +%H:%M:%S) $STATE"
  case "$STATE" in
    SUCCESS) break ;;
    FAILED|CANCELLED)
      echo "Job $STATE. Driver logs:"
      echo "  aws s3 ls --recursive s3://$LAKE_BUCKET/logs/applications/$APP_ID/jobs/$JOB_RUN_ID/"
      exit 1 ;;
  esac
  sleep 20
done

echo "Curated tables:"
aws s3 ls "s3://$LAKE_BUCKET/curated/"

# 4. Register / update the tables in the Glue Data Catalog for Athena.
if [[ "${CRAWL:-1}" == "1" ]]; then
  CRAWLER="$(tf glue_crawler 2>/dev/null || true)"
  if [[ -n "$CRAWLER" && "$CRAWLER" != "null" ]]; then
    aws glue start-crawler --name "$CRAWLER"
    echo "Started Glue crawler $CRAWLER. Tables appear in Athena (workgroup $(tf athena_workgroup)) in ~1-2 min."
  fi
fi
