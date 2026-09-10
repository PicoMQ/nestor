resource "aws_vpc" "this" {
  count = local.create_network ? 1 : 0

  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = var.project }
}

resource "aws_subnet" "this" {
  count = local.create_network ? 1 : 0

  vpc_id                  = aws_vpc.this[0].id
  cidr_block              = "10.0.0.0/24"
  map_public_ip_on_launch = true

  tags = { Name = var.project }
}

resource "aws_internet_gateway" "this" {
  count = local.create_network ? 1 : 0

  vpc_id = aws_vpc.this[0].id

  tags = { Name = var.project }
}

resource "aws_route_table" "this" {
  count = local.create_network ? 1 : 0

  vpc_id = aws_vpc.this[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this[0].id
  }

  tags = { Name = var.project }
}

resource "aws_route_table_association" "this" {
  count = local.create_network ? 1 : 0

  subnet_id      = aws_subnet.this[0].id
  route_table_id = aws_route_table.this[0].id
}

data "aws_route_table" "existing" {
  count = local.create_network || !var.create_s3_endpoint ? 0 : 1

  subnet_id = var.subnet_id
}

resource "aws_vpc_endpoint" "s3" {
  count = local.create_s3_endpoint ? 1 : 0

  vpc_id            = local.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids = local.create_network ? (
    [aws_route_table.this[0].id]
    ) : (
    [data.aws_route_table.existing[0].id]
  )

  tags = { Name = "${var.project}-s3" }
}

resource "aws_security_group" "this" {
  name_prefix = "${var.project}-"
  vpc_id      = local.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = var.project }
}
