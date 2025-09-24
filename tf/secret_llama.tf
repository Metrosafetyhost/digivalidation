# Secret container
resource "aws_secretsmanager_secret" "llama" {
  name        = "${module.lambdas_zip.namespace}-llama-cloud-api-key"
  description = "LlamaParse (LlamaIndex) API key for ${module.lambdas_zip.namespace}"
}

# Current secret value (as JSON so we can use dynamic reference by key)
resource "aws_secretsmanager_secret_version" "llama_current" {
  secret_id     = aws_secretsmanager_secret.llama.id
  secret_string = jsonencode({
    LLAMA_CLOUD_API_KEY = var.llama_cloud_api_key
  })
}
