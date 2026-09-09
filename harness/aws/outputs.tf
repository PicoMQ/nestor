output "instance_id" {
  value = aws_instance.bench.id
}

output "instance_type" {
  value = var.instance_type
}

output "bucket" {
  value = aws_s3_bucket.bench.bucket
}

output "region" {
  value = var.region
}
