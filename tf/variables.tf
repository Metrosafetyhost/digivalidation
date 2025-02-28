variable "namespace" {
  description = "Namespace of the service"
  type        = string
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
  default = "ap-southeast-2"
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
