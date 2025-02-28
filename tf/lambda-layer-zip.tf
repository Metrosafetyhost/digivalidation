module "lambda_layer" {
  source = "./modules/lambda-layer"

  namespace     = var.namespace
  s3_zip_bucket = module.lambda_zips_bucket.this_s3_bucket_id
  common_tags   = local.common_tags
}
