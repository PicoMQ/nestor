resource "aws_s3_bucket" "bench" {
  bucket_prefix = "nestor-bench-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "bench" {
  bucket                  = aws_s3_bucket.bench.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "bench" {
  bucket = aws_s3_bucket.bench.id

  rule {
    id     = "abandoned-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}
