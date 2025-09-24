variable "namespace" {
  description = "Namespace of the service"
  type        = string
  default     = "bedrock-lambda"
}

variable "repo_name" {
  type    = string
  default = "data-kapua-template"
}

variable "env" {
  description = "AWS Environment to deploy too"
  type        = string
  default     = "data-dev"
}

variable "gh_org" {
  type    = string
  default = "trove-data"
}

variable "region" {
  type    = string
  default = "eu-west-2"
}

variable "lambda_file_names" {
  description = "The file names of the Lambda functions"
  type        = list(string)
}

variable "lambda_names" {
  description = "The names of the Lambda functions without suffix aka w/o .py/.js"
  type        = list(string)
}

variable "runtime" {
  type = string
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

# Provide via TF Cloud/Workspace vars or CI; do NOT hardcode
# variable "openai_api_key" {
#   type      = string
#   sensitive = true
# }

# Name
variable "openai_secret_name" {
  type    = string
  default = "openai/api_key"
}

# Lambdas that should have OpenAI enabled (layer + secret access)
variable "openai_enabled_lambdas" {
  type    = list(string)
  default = ["asset_categorisation"] # add more later as needed
}

# OpenAI layer ARN (eu-west-2 ARM64 for your runtime)
variable "openai_layer_arn" {
  type    = string
  default = "arn:aws:lambda:eu-west-2:837329614132:layer:openai-py-313-arm64:1"
}

# Which lambdas are allowed to read the Llama secret
variable "llama_enabled_lambdas" {
  type        = list(string)
  description = "Lambda names that can read the Llama (LlamaParse) secret"
  default     = ["llamaparse"] # add more if needed
}

# The secret value you'll pass at apply-time (never commit this)
variable "llama_cloud_api_key" {
  type        = string
  description = "LlamaParse API key (LLAMA_CLOUD_API_KEY)"
  sensitive   = true
}

