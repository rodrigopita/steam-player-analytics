# The raw zone is the one part of the system that cannot be rebuilt, so it is
# the one part that lives off the laptop. Versioning is the safety net behind
# deterministic keys: a rerun overwrites, and the overwritten version stays.
resource "aws_s3_bucket" "raw" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# The pipeline identity. Append-only by policy, not by good intentions: it can
# put, get and list, and no statement grants delete. With versioning on, even
# an overwrite adds a version rather than removing one. Removing history takes
# the account owner.
resource "aws_iam_user" "airflow" {
  name = "steam-analytics-airflow"
}

data "aws_iam_policy_document" "s3_raw_rw" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*"]
  }
}

resource "aws_iam_user_policy" "s3_raw_rw" {
  name   = "s3-raw-rw"
  user   = aws_iam_user.airflow.name
  policy = data.aws_iam_policy_document.s3_raw_rw.json
}

resource "aws_iam_access_key" "airflow" {
  user = aws_iam_user.airflow.name
}
