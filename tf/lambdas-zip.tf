module "lambdas_zip" {
  source    = "./modules/lambdas-zip"
  namespace = var.namespace
  repo_name = var.repo_name
  env       = var.env

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
    "emails",
    "llamaparse",
  ]

  # these are the Python files that get zipped
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
    "emails.py",
    "llamaparse.py",
  ]

  runtime       = "python3.13"
  arch          = "arm64"
  s3_zip_bucket = module.lambda_zips_bucket.this_s3_bucket_id
  build_dir     = var.build_dir

  default_environment = {
    OPENAI_MODEL = "gpt-4o-mini"
  }

  # Layers
  lambda_layer_arns = [
    module.lambda_layer.lambda_layer_arn,   # your shared deps
    var.openai_layer_arn,                   # OpenAI layer (keep if others use it)
    aws_lambda_layer_version.llamaindex.arn #LlamaIndex/LlamaParse deps
  ]

  force_lambda_code_deploy = true

  lambda_config = {
    asset_categorisation = {
      handler     = "process"
      memory_size = 512
      timeout     = 240
      lambda_environment = {
        OPENAI_SECRET_ARN = aws_secretsmanager_secret.openai.arn
      }
    }

    # LlamaParse lambda
    llamaparse = {
      handler     = "process"
      timeout     = 120
      memory_size = 1024
      lambda_environment = {
        # Secrets Manager *dynamic reference* to the JSON key
        LLAMA_CLOUD_API_KEY = "{{resolve:secretsmanager:${aws_secretsmanager_secret.llama.arn}:SecretString:LLAMA_CLOUD_API_KEY}}"
      }
    }

    # All other lambdas 
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
    emails                 = { handler = "process", timeout = 240, memory_size = 512 }
  }
}
