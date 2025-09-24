# Secret container
resource "aws_secretsmanager_secret" "llama" {
  name        = "${module.lambdas_zip.namespace}-llama-cloud-api-key"
  description = "LlamaParse (LlamaIndex) API key for ${module.lambdas_zip.namespace}"
}
