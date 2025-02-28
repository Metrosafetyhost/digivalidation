variable "namespace" {
  type        = string
  description = "The namespace for the resources"
}

variable "arch" {
  type        = string
  description = "arch"
  default     = "arm64"
}

variable "runtime" {
  type        = string
  description = "The runtime to use for the function"
  default     = "python3.11"
}

variable "layer_staging_dir" {
  type        = string
  description = "The directory where the runtime layer packages are staged"
  default     = "layer"
}

variable "s3_zip_bucket" {
  type        = string
  description = "AWS S3 Bucket name for lambda and layer zips"
}

variable "force_layer_code_deploy" {
  type        = bool
  description = "Force the layer code to be deployed"
  default     = false
}

variable "common_tags" {
  type = object({
    ManagedBy = string
    git_repo  = string
    git_org   = string
  })
  default = {
    ManagedBy = "terraform"
    git_repo  = "trove-terraform-modules"
    git_org   = "trovemoney"
  }
}
