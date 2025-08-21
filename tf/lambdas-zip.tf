module "lambdas_zip" {
  source = "./modules/lambdas-zip"

  namespace  = var.namespace
  repo_name  = var.repo_name
  env        = var.env

  lambda_names       = [  "asset_categorisation",
                          "checklist",
                          "basic_event",
                          "bedrock",
                          "categorisation",
                          "checklist_proofing",
                          "config",
                          "db",
                          "digival",
                          "fra_checklist_proofing",
                          "hsa_checklist_proofing",
                          "salesforce_input",]
  lambda_file_names  = [  "asset_categorisation",
                          "checklist",
                          "basic_event",
                          "bedrock",
                          "categorisation",
                          "checklist_proofing",
                          "config",
                          "db",
                          "digival",
                          "fra_checklist_proofing",
                          "hsa_checklist_proofing",
                          "salesforce_input",]

  runtime = "python3.13"
  arch    = "arm64"

  s3_zip_bucket = module.lambda_zips_bucket.this_s3_bucket_id

  build_dir     = var.build_dir

  default_environment = {
    OPENAI_MODEL = "gpt-4o-mini"
  }

  lambda_layer_arn = var.openai_layer_arn # or module.lambda_layer.lambda_layer_arn

  force_lambda_code_deploy = true

  lambda_config = {
    asset_categorisation = {
      handler            = "process"
      memory_size        = 512
      timeout            = 240
      lambda_environment = {
        OPENAI_SECRET_ARN = aws_secretsmanager_secret.openai.arn
      }
    }
    # For all the other legacy lambdas, set the handler they actually use today:
  basic_event = { handler = "process" }
  bedrock     = { handler = "process" }
  categorisation = { handler = "process" }
  checklist   = { handler = "process" }
  checklist_proofing = { handler = "process" }
  config      = { handler = "process" }
  db          = { handler = "process" }
  digival     = { handler = "process" }
  fra_checklist_proofing = { handler = "process" }
  hsa_checklist_proofing = { handler = "process" }
  salesforce_input = { handler = "process" }

  }
}

