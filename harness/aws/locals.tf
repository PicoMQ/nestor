locals {
  create_network      = var.vpc_id == null
  vpc_id              = local.create_network ? aws_vpc.this[0].id : var.vpc_id
  subnet_id           = local.create_network ? aws_subnet.this[0].id : var.subnet_id
  create_s3_endpoint  = local.create_network || var.create_s3_endpoint
  associate_public_ip = coalesce(var.associate_public_ip, local.create_network)
}
