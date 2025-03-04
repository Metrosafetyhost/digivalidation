module "bedrock_output_bucket" {
  source = "./modules/s3"

  bucket         = "metrosafety-bedrock-output-${var.env}-${var.namespace}"
  env            = var.env
  encrypt_bucket = false

  versioning = {
    enabled    = "Enabled"
    mfa_delete = "Disabled"
  }

  force_destroy = true
}

resource "aws_bedrock_model" "anthropic_claude_v2" {
  model_id = "anthropic.claude-v2" # Adjust to the best available model
}
