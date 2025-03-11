resource "aws_dynamodb_table" "proofing_metadata" {
  name         = "ProofingMetadata"
  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "workorder_id"
    type = "S"
  }

  hash_key = "workorder_id"

  tags = {
    Name        = "ProofingMetadata"
    Environment = var.env
  }
}

resource "aws_iam_policy" "lambda_dynamodb_access" {
  name        = "LambdaDynamoDBAccess"
  description = "Allows Lambda to write to the ProofingMetadata DynamoDB table"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Scan",
          "dynamodb:Query"
        ],
        Resource = "arn:aws:dynamodb:eu-west-2:837329614132:table/ProofingMetadata"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_dynamodb" {
  policy_arn = aws_iam_policy.lambda_dynamodb_access.arn
  role       = "bedrock-lambda-salesforce_input"
}
