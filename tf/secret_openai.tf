# Create the secret container
resource "aws_secretsmanager_secret" "openai" {
  name = var.openai_secret_name
  tags = { app = "openai" }
}

resource "aws_secretsmanager_secret" "dewrra_api_key" {
  name = var.dewrra_api_key_secret_name
  tags = { app = "dewrra" }
}
