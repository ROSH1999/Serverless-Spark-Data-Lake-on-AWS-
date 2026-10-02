# Read by ../scripts/*.sh

output "region" {
  value = var.region
}

output "raw_bucket" {
  value = aws_s3_bucket.this["raw"].bucket
}

output "lake_bucket" {
  value = aws_s3_bucket.this["lake"].bucket
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_job_role_arn" {
  value = aws_iam_role.emr_job.arn
}

output "glue_database" {
  value = var.enable_glue_catalog ? aws_glue_catalog_database.lake[0].name : null
}

output "glue_crawler" {
  value = var.enable_glue_catalog ? aws_glue_crawler.curated[0].name : null
}

output "athena_workgroup" {
  value = var.enable_glue_catalog ? aws_athena_workgroup.lake[0].name : null
}
