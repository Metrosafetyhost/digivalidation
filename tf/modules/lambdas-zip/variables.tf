variable "namespace" {
  description = "Namespace of the service"
  type        = string
}

variable "repo_name" {
  type = string
}

variable "env" {
  type = string
}

variable "gh_org" {
  default = "trove-data"
}

variable "lambda_file_names" {
  description = "The file names of the Lambda functions"
  type        = list(string)
}

variable "lambda_names" {
  description = "The names of the Lambda functions without suffix used as s3 key"
  type        = list(string)
}

variable "s3_zip_bucket" {
  description = "The S3 bucket to store the lambda zips"
  type        = string
}

variable "lambdas_dir" {
  type        = string
  description = "The directory where the lambda code is"
  default     = "lambdas"
}

variable "build_dir" {
  type        = string
  description = "The directory where the lambda code is built"
  default     = "dist"
}

variable "lambda_layer_arn" {
  description = "Lambda Layer ARN from the lambda-layer module"
  type        = string
  default     = ""
}

# -- ARNs of Triggers (map per lambda)
variable "lambda_event_sources" {
  description = "A nested map of Lambda function names to their event source configurations"
  type = map(map(object({
    type       = string
    source_arn = string
    rules = optional(list(object({
      name  = string
      value = string
    })))
  })))
  default = {}
}

variable "runtime" {
  description = "The runtime to use for the function"
  type        = string
  default     = "python3.11"
}

variable "arch" {
  type        = string
  description = "The architecture of the Lambda function"
  default     = "arm64"
}

variable "lambda_config" {
  description = "A map of Lambda function names to their specific configurations"
  type = map(object({
    memory_size        = optional(number, 512)
    timeout            = optional(number, 6)
    handler            = optional(string, "process")
    lambda_environment = optional(map(string), {})
    runtime            = optional(string)
    arch               = optional(string)
    lambda_layers      = optional(list(string), [])
  }))
  default = {}
}

# Lambda Environment Variables
variable "handler" {
  type        = string
  description = "All functions entrypoint in trove code."
  default     = "process"
}

variable "default_environment" {
  type    = map(string)
  default = {}
}

variable "force_lambda_code_deploy" {
  type        = bool
  description = "Force the lambda code to be deployed"
  default     = false
}


# IAM Role: Allow Override or Use Default
variable "lambda_role_arn" {
  type        = string
  description = "Optional IAM Role ARN for the Lambda function. If null, a default role is created."
  default     = null
}


# Additional IAM Policies to Attach to the Lambda Role
variable "lambda_additional_policies" {
  type        = list(string)
  description = "List of additional IAM policies to attach to the Lambda function role."
  default     = []
}

variable "log_retention" {
  type        = number
  description = "Cloudwatch log retention"
  default     = 30
}

# NEW: allow passing multiple layers to ALL lambdas
variable "lambda_layer_arns" {
  type    = list(string)
  default = []
}

