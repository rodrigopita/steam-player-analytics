output "raw_bucket" {
  description = "Value for AIRFLOW_VAR_RAW_BUCKET in .env."
  value       = aws_s3_bucket.raw.bucket
}

# On a state that imported an existing key the password is null: AWS returns
# a secret once, at creation. A fresh apply fills it in.
output "airflow_conn_aws_default" {
  description = "Value for AIRFLOW_CONN_AWS_DEFAULT in .env; read with terraform output -raw."
  sensitive   = true
  value = jsonencode({
    conn_type = "aws"
    login     = aws_iam_access_key.airflow.id
    password  = aws_iam_access_key.airflow.secret
    extra     = { region_name = var.region }
  })
}
