resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

module "lambda_zips_bucket" {
  source = "./modules/s3"

  bucket         = "metrosafety-lambdazips-${var.env}-${var.namespace}-${random_string.suffix.result}"
  env            = var.env
  encrypt_bucket = false

  versioning = {
    enabled    = "Enabled"
    mfa_delete = "Disabled"
  }

  force_destroy = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lambda_zips_bucket_versioning" {
  bucket = module.lambda_zips_bucket.this_s3_bucket_id

  rule {
    id     = "versions"
    status = "Enabled"
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}
