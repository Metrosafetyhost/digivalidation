module "lambda_layer" {
  source = "./modules/lambda-layer"
 

  namespace               = var.namespace
  s3_zip_bucket           = module.lambda_zips_bucket.this_s3_bucket_id
  force_layer_code_deploy = true
  common_tags              = local.common_tags
}
