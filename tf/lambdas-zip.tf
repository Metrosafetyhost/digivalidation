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

resource "aws_lambda_function" "salesforce_input" {
  function_name    = "bedrock-lambda-salesforce_input"
  runtime         = "python3.9"
  handler         = "salesforce_input.process"
  role            = aws_iam_role.lambda_role.arn
  timeout         = 60 
  memory_size     = 1024 

  environment {
    variables = {
      BEDROCK_MODEL_ID = "amazon.titan-text-lite-v1"
    }
  }

  filename         = "dist/salesforce_input.zip"
  source_code_hash = filebase64sha256("dist/salesforce_input.zip")
}
