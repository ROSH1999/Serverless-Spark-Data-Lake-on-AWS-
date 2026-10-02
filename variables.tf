variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Prefix for resource names. Lowercase letters, digits and hyphens."
  type        = string
  default     = "sparkify-lake"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,25}$", var.project))
    error_message = "Use 3-26 lowercase letters, digits or hyphens, starting with a letter."
  }
}

variable "emr_release_label" {
  description = "EMR Serverless release. emr-7.14.0 ships Spark 3.5.8; see the EMR Serverless release notes for newer labels."
  type        = string
  default     = "emr-7.14.0"
}

variable "max_vcpu" {
  description = "Hard cap on vCPUs the Spark application can use at once (cost guard-rail)."
  type        = number
  default     = 8
}

variable "max_memory_gb" {
  description = "Hard cap on memory (GB) the Spark application can use at once."
  type        = number
  default     = 32
}

variable "enable_glue_catalog" {
  description = "Create a Glue database + crawler and an Athena workgroup so you can query the curated tables with SQL."
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Days to keep EMR job logs in the lake bucket."
  type        = number
  default     = 14
}
