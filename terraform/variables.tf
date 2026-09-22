variable "bucket_name" {
  description = "Raw-zone bucket. Same value as AIRFLOW_VAR_RAW_BUCKET in .env."
  type        = string
}

variable "owner" {
  description = "Value of the owner tag on every resource, lowercase kebab-case."
  type        = string
}

variable "region" {
  description = "Bucket region. Same value as region_name in the AWS connection."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Value of the environment tag."
  type        = string
  default     = "prod"
}
