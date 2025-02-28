terraform {
  required_version = ">= 1.5"
  backend "s3" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.84.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0.4"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}


locals {
  this_account = data.aws_caller_identity.current.account_id
}
