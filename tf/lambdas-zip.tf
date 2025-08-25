module "lambdas_zip" {
  source = "./modules/lambdas-zip"

  namespace  = var.namespace
  repo_name  = var.repo_name
  env        = var.env

  lambda_names = [
    "asset_categorisation",
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
    "salesforce_input",
  ]

  # IMPORTANT: these must be the zip *file names* that exist in your build dir
  lambda_file_names = [
    "asset_categorisation.py",
    "checklist.py",
    "basic_event.py",
    "bedrock.py",
    "categorisation.py",
    "checklist_proofing.py",
    "config.py",
    "db.py",
    "digival.py",
    "fra_checklist_proofing.py",
    "hsa_checklist_proofing.py",
    "salesforce_input.py",
  ]

  runtime       = "python3.13"
  arch          = "arm64"
  s3_zip_bucket = module.lambda_zips_bucket.this_s3_bucket_id
  build_dir     = var.build_dir

  # Model name is harmless on non-AI lambdas
  default_environment = {
    OPENAI_MODEL = "gpt-4o-mini"
  }

  # NEW: give EVERY lambda the two layers (your deps + OpenAI)
  lambda_layer_arns = [
    module.lambda_layer.lambda_layer_arn,  # your existing deps layer (bs4, requests, etc.)
    var.openai_layer_arn                   # OpenAI Python layer
  ]

  force_lambda_code_deploy = true

  # Handlers (+ any per-lambda env). Use the real entrypoints your files expose.
  lambda_config = {
    # Only this one actually needs the OpenAI secret
    asset_categorisation = {
      handler            = "process"    # matches asset_categorisation.py
      memory_size        = 512
      timeout            = 240
      lambda_environment = {
        OPENAI_SECRET_ARN = aws_secretsmanager_secret.openai.arn
      }
    }

    # All the others keep their current handler names (you said they use "process")
    basic_event            = { handler = "process", timeout = 240, memory_size = 512 }
    bedrock                = { handler = "process", timeout = 240, memory_size = 512 }
    categorisation         = { handler = "process", timeout = 240, memory_size = 512 }
    checklist              = { handler = "process", timeout = 240, memory_size = 512 }
    checklist_proofing     = { handler = "process", timeout = 240, memory_size = 512 }
    config                 = { handler = "process", timeout = 240, memory_size = 512 }
    db                     = { handler = "process", timeout = 240, memory_size = 512 }
    digival                = { handler = "process", timeout = 240, memory_size = 512 }
    fra_checklist_proofing = { handler = "process", timeout = 240, memory_size = 512 }
    hsa_checklist_proofing = { handler = "process", timeout = 240, memory_size = 512 }
    salesforce_input       = { handler = "process", timeout = 240, memory_size = 512 }
  }
}
