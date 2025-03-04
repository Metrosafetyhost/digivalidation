resource "aws_iam_policy" "bedrock_invoke_policy" {
  name        = "BedrockInvokePolicy"
  description = "Allows AWS Lambda to call AWS Bedrock models"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "bedrock:InvokeModel"
        Resource = "arn:aws:bedrock:eu-west-2::foundation-model/amazon.titan-text-lite-v1"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_bedrock_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.bedrock_invoke_policy.arn
}
