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
    "nova_water",
    "waterRiskCaseIngest",
    "pdf_qa",
    "pdfqa_api",
    "blur_image",
    "geocoding",
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
    "nova_water.py",
    "waterRiskCaseIngest.py",
    "pdf_qa.py",
    "pdfqa_api.py",
    "blur_image.py",
    "geocoding.py",

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
    module.lambda_layer.lambda_layer_arn, # your shared deps
    var.openai_layer_arn,                 # OpenAI layer (keep if others use it)
    aws_lambda_layer_version.pymupdf.arn
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

  pdf_qa = {
    handler     = "process"
    timeout     = 240
    memory_size = 512

    lambda_environment = {
      OPENAI_SECRET_ARN = aws_secretsmanager_secret.openai.arn
      //DEWRRA_API_KEY_SECRET_ARN   = aws_secretsmanager_secret.dewrra_api_key.arn
    }
  }

    pdfqa_api = {
    handler     = "process"
    timeout     = 30
    memory_size = 256

    # lambda_environment = {
    #   DEWRRA_JOBS_TABLE    = "dewrra_jobs"
    #   DEWRRA_QUEUE_URL     = aws_sqs_queue.dewrra_jobs_queue.id   # or .url depending on provider version; usually .url
    #   DEWRRA_RESULT_PREFIX = "dewrra/results/"

    #   # If your API lambda will also validate API keys:
    #   # DEWRRA_API_KEY_SECRET_ARN = aws_secretsmanager_secret.dewrra_api_key.arn
    # }
  }

    # All other lambdas 
    basic_event            = { handler = "process", timeout = 240, memory_size = 512 }
    bedrock                = { handler = "process", timeout = 240, memory_size = 512 }
    categorisation         = { handler = "process", timeout = 240, memory_size = 512 }
    checklist              = { handler = "process", timeout = 500, memory_size = 512 }
    checklist_proofing     = { handler = "process", timeout = 240, memory_size = 512 }
    config                 = { handler = "process", timeout = 240, memory_size = 512 }
    db                     = { handler = "process", timeout = 240, memory_size = 512 }
    digival                = { handler = "process", timeout = 240, memory_size = 512 }
    fra_checklist_proofing = { handler = "process", timeout = 240, memory_size = 512 }
    hsa_checklist_proofing = { handler = "process", timeout = 240, memory_size = 512 }
    salesforce_input       = { handler = "process", timeout = 240, memory_size = 512 }
    emails                 = { handler = "process", timeout = 240, memory_size = 512 }
    nova_water             = { handler = "process", timeout = 900, memory_size = 512 }
    waterRiskCaseIngest    = { handler = "process", timeout = 900, memory_size = 512 }
    blur_image             = { handler = "process", timeout = 240, memory_size = 512 }
    geocoding              = { handler = "process", timeout = 240, memory_size = 512 }
  }
}
