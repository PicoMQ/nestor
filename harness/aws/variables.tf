variable "region" {
  description = "Region for the instance and the bucket. Both are placed together so origin latency is one region's S3."
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "A type with local NVMe instance store gives the disk tier a realistic device. Without one the tier falls back to the root volume."
  type        = string
  default     = "i4i.xlarge"
}

variable "root_volume_gb" {
  description = "Root volume, holds the toolchain and the cargo registry. Build output goes to the NVMe when there is one."
  type        = number
  default     = 30
}

variable "repository" {
  description = "Git URL cloned on the instance."
  type        = string
  default     = "https://github.com/PicoMQ/nestor.git"
}
