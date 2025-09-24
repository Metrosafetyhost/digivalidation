# Look up each execution role created by your lambdas-zip module
data "aws_iam_role" "llama_lambda_roles" {
  for_each = toset(var.llama_enabled_lambdas)
  name     = "${module.lambdas_zip.namespace}-${each.key}"
}

# Grant GetSecretValue on the LLAMA secret to those roles
resource "aws_iam_role_policy" "allow_get_llama_secret" {
  for_each = data.aws_iam_role.llama_lambda_roles

  name = "AllowGetLlamaSecret"
  role = each.value.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["secretsmanager:GetSecretValue"],
      Resource = aws_secretsmanager_secret.llama.arn
    }]
  })
}
