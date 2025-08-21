# Create the secret container
resource "aws_secretsmanager_secret" "openai" {
  name = var.openai_secret_name
  tags = { app = "openai" }
}


