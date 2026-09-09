data "aws_ec2_instance_type" "bench" {
  instance_type = var.instance_type
}

locals {
  arch = contains(data.aws_ec2_instance_type.bench.supported_architectures, "arm64") ? "arm64" : "x86_64"
}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${local.arch}"
}

resource "aws_instance" "bench" {
  ami                    = nonsensitive(data.aws_ssm_parameter.al2023.value)
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.bench.id
  vpc_security_group_ids = [aws_security_group.bench.id]
  iam_instance_profile   = aws_iam_instance_profile.bench.name

  user_data = templatefile("${path.module}/cloud-init.yaml", {
    region     = var.region
    bucket     = aws_s3_bucket.bench.bucket
    repository = var.repository
  })
  user_data_replace_on_change = true

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_gb
  }

  tags = { Name = "nestor-bench" }
}
