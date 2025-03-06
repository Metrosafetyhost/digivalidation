module "lambdas_zip" {
  source = "./modules/lambdas-zip"

  namespace           = var.namespace
  env                 = var.env
  repo_name           = var.repo_name
  lambda_names        = var.lambda_names
  lambda_file_names   = var.lambda_file_names
  runtime             = var.runtime
  s3_zip_bucket       = module.lambda_zips_bucket.this_s3_bucket_id
  lambda_layer_arn    = module.lambda_layer.lambda_layer_arn
  force_lambda_code_deploy = true
  # lambda_additional_policies = ["data_extra_lambda_policy"]
}
