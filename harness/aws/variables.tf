variable "project" {
  type    = string
  default = "nestor"
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "instance_type" {
  type    = string
  default = "i4i.xlarge"
}

variable "root_volume_gb" {
  type    = number
  default = 20
}

variable "image" {
  type    = string
  default = "ghcr.io/picomq/nestor:latest"
}

variable "vpc_id" {
  type    = string
  default = null
}

variable "subnet_id" {
  type    = string
  default = null

  validation {
    condition     = (var.vpc_id == null) == (var.subnet_id == null)
    error_message = "Set vpc_id and subnet_id together, or omit both to create a network."
  }
}

variable "associate_public_ip" {
  type    = bool
  default = null
}

variable "create_s3_endpoint" {
  type    = bool
  default = false
}

variable "force_destroy" {
  type    = bool
  default = true
}
