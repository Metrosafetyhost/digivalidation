# The execution roles are created by your lambdas-zip module as:
#   <namespace>-<lambda_name>
# Example: bedrock-lambda-asset_categorisation
# We look up a role for each OpenAI-enabled lambda:

data "aws_iam_role" "openai_lambda_roles" {
  for_each = toset(var.openai_enabled_lambdas)
  name     = "${module.lambdas_zip.namespace}-${each.key}"
}

# Attach permission to read the secret for those roles only
resource "aws_iam_role_policy" "allow_get_openai_secret" {
  for_each = data.aws_iam_role.openai_lambda_roles

  name = "AllowGetOpenAISecret"
  role = each.value.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["secretsmanager:GetSecretValue"],
      Resource = aws_secretsmanager_secret.openai.arn
    }]
  })
}
