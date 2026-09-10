data "aws_ec2_instance_type" "this" {
  instance_type = var.instance_type
}

locals {
  arch = contains(data.aws_ec2_instance_type.this.supported_architectures, "arm64") ? "arm64" : "x86_64"
}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-${local.arch}"
}

resource "aws_instance" "this" {
  ami                         = nonsensitive(data.aws_ssm_parameter.al2023.value)
  instance_type               = var.instance_type
  subnet_id                   = local.subnet_id
  vpc_security_group_ids      = [aws_security_group.this.id]
  iam_instance_profile        = aws_iam_instance_profile.this.name
  associate_public_ip_address = local.associate_public_ip

  user_data = templatefile("${path.module}/cloud-init.yaml", {
    nestor_toml_b64    = base64encode(templatefile("${path.module}/nestor.toml", { region = var.region }))
    nestor_service_b64 = base64encode(templatefile("${path.module}/nestor.service", { image = var.image }))
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

  tags = { Name = var.project }
}
