# Infrastructure for the data lake project:
#
#   s3://<raw>/raw/{song_data,log_data}/       ← upload_raw.sh
#            │ read
#            ▼
#   EMR Serverless Spark app  (runs etl.py as the job role)
#            │ write Parquet
#            ▼
#   s3://<lake>/curated/{songs,artists,users,time,songplays}/
#            │ crawl (optional)
#            ▼
#   Glue Data Catalog  ──►  Athena workgroup (query with SQL)
#
# EMR Serverless has no cluster to manage or pay for while idle: workers start
# when a job is submitted and the application auto-stops afterwards.

data "aws_partition" "current" {}

locals {
  tables = ["songs", "artists", "users", "time", "songplays"]
}

# ------------------------------------------------------------------ buckets

resource "aws_s3_bucket" "this" {
  for_each      = toset(["raw", "lake"])
  bucket_prefix = "${var.project}-${each.key}-"
  force_destroy = true # lets `terraform destroy` clean up sample data
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Job logs and Athena results pile up quickly; expire them automatically.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.this["lake"].id

  rule {
    id     = "expire-job-logs"
    status = "Enabled"
    filter {
      prefix = "logs/"
    }
    expiration {
      days = var.log_retention_days
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }
}

# --------------------------------------------------- EMR Serverless job role

data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "emr_job" {
  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [for b in aws_s3_bucket.this : b.arn]
  }
  statement {
    sid       = "ReadRaw"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.this["raw"].arn}/*"]
  }
  statement {
    sid       = "ReadWriteLake"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.this["lake"].arn}/*"]
  }
}

resource "aws_iam_role" "emr_job" {
  name_prefix        = "${var.project}-emr-job-"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

resource "aws_iam_role_policy" "emr_job" {
  name   = "lake-access"
  role   = aws_iam_role.emr_job.id
  policy = data.aws_iam_policy_document.emr_job.json
}

# ------------------------------------------------------ EMR Serverless app

resource "aws_emrserverless_application" "spark" {
  name          = "${var.project}-spark"
  release_label = var.emr_release_label
  type          = "spark"

  maximum_capacity {
    cpu    = "${var.max_vcpu} vCPU"
    memory = "${var.max_memory_gb} GB"
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 5
  }
}

# ------------------------------------------- Glue catalog + Athena (optional)

resource "aws_glue_catalog_database" "lake" {
  count       = var.enable_glue_catalog ? 1 : 0
  name        = replace("${var.project}_curated", "-", "_")
  description = "Curated Sparkify tables written by the Spark ETL"
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_read_lake" {
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.this["lake"].arn, "${aws_s3_bucket.this["lake"].arn}/curated/*"]
  }
}

resource "aws_iam_role" "glue_crawler" {
  count              = var.enable_glue_catalog ? 1 : 0
  name_prefix        = "${var.project}-crawler-"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  count      = var.enable_glue_catalog ? 1 : 0
  role       = aws_iam_role.glue_crawler[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_read_lake" {
  count  = var.enable_glue_catalog ? 1 : 0
  name   = "read-curated"
  role   = aws_iam_role.glue_crawler[0].id
  policy = data.aws_iam_policy_document.glue_read_lake.json
}

# One target per table folder -> one catalog table each, with year/month
# partitions discovered from the Hive-style paths.
resource "aws_glue_crawler" "curated" {
  count         = var.enable_glue_catalog ? 1 : 0
  name          = "${var.project}-curated"
  role          = aws_iam_role.glue_crawler[0].arn
  database_name = aws_glue_catalog_database.lake[0].name

  dynamic "s3_target" {
    for_each = toset(local.tables)
    content {
      path = "s3://${aws_s3_bucket.this["lake"].bucket}/curated/${s3_target.value}/"
    }
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

resource "aws_athena_workgroup" "lake" {
  count         = var.enable_glue_catalog ? 1 : 0
  name          = var.project
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["lake"].bucket}/athena-results/"
    }
  }
}
