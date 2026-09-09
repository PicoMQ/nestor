resource "aws_vpc" "bench" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "nestor-bench" }
}

resource "aws_subnet" "bench" {
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = "10.0.0.0/24"
  map_public_ip_on_launch = true

  tags = { Name = "nestor-bench" }
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id

  tags = { Name = "nestor-bench" }
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }

  tags = { Name = "nestor-bench" }
}

resource "aws_route_table_association" "bench" {
  subnet_id      = aws_subnet.bench.id
  route_table_id = aws_route_table.bench.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.bench.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.bench.id]

  tags = { Name = "nestor-bench-s3" }
}

resource "aws_security_group" "bench" {
  name        = "nestor-bench"
  description = "No inbound. The instance is reached through SSM."
  vpc_id      = aws_vpc.bench.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "nestor-bench" }
}
