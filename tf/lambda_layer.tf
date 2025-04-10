resource "aws_iam_role" "lambda_role" {
  name               = "bedrock_lambda_execution_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}


resource "aws_iam_policy" "bedrock_invoke_policy" {
  name        = "BedrockInvokePolicy"
  description = "Allows AWS Lambda to call AWS Bedrock models"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "bedrock:InvokeModel"
        Resource = "arn:aws:bedrock:eu-west-2::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0" #anthropic.claude-3-haiku-20240307-v1:0"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_bedrock_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.bedrock_invoke_policy.arn
}

resource "aws_iam_policy" "lambda_s3_access" {
  name        = "LambdaS3WriteAccess"
  description = "Allows Lambda to write proofing data to S3"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::metrosafety-bedrock-output-data-dev-bedrock-lambda",
          "arn:aws:s3:::metrosafety-bedrock-output-data-dev-bedrock-lambda/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3" {
  policy_arn = aws_iam_policy.lambda_s3_access.arn
  role       = "bedrock-lambda-salesforce_input"
}


#############################
# Unified Lambda Execution Role
#############################

resource "aws_iam_role" "bedrock_lambda_checklist" {
  name = "bedrock-lambda-checklist"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

#############################
# Textract Policy for Lambda
#############################

resource "aws_iam_policy" "lambda_textract_policy" {
  name   = "LambdaTextractPolicy"
  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "textract:AnalyzeDocument",
          "textract:StartDocumentAnalysis",
          "textract:GetDocumentAnalysis"
        ],
        Resources = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_textract_to_lambda" {
  role       = aws_iam_role.bedrock_lambda_checklist.name
  policy_arn = aws_iam_policy.lambda_textract_policy.arn
}

#############################
# S3 Read Policy for Lambda
#############################

data "aws_iam_policy_document" "lambda_s3_read" {
  statement {
    sid    = "AllowLambdaS3Read"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:HeadObject",
      "s3:ListBucket"
    ]
    resources = [
      "arn:aws:s3:::metrosafetyprodfiles",
      "arn:aws:s3:::metrosafetyprodfiles/*"
    ]
  }
}

resource "aws_iam_policy" "lambda_s3_read_policy" {
  name   = "LambdaS3ReadPolicy"
  policy = data.aws_iam_policy_document.lambda_s3_read.json
}

resource "aws_iam_role_policy_attachment" "attach_lambda_s3_read" {
  role       = aws_iam_role.bedrock_lambda_checklist.name
  policy_arn = aws_iam_policy.lambda_s3_read_policy.arn
}

#############################
# Output the Lambda Role Name
#############################

output "lambda_role_name" {
  value = aws_iam_role.bedrock_lambda_checklist.name
}
