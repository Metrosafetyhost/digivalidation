data "aws_caller_identity" "current" {}

resource "aws_iam_user" "salesforce_s3_file_viewer" {
  name = "salesforce-s3-file-viewer"
}

resource "aws_iam_policy" "salesforce_s3_file_viewer_invoke" {
  name        = "salesforce-s3-file-viewer-invoke"
  description = "Allows Salesforce to invoke only the S3 file viewer API routes"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Action = [
          "execute-api:Invoke"
        ]

        Resource = [
          "arn:aws:execute-api:eu-west-2:${data.aws_caller_identity.current.account_id}:${aws_apigatewayv2_api.lambda_api.id}/prod/*/files/*"
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "salesforce_s3_file_viewer" {
  user       = aws_iam_user.salesforce_s3_file_viewer.name
  policy_arn = aws_iam_policy.salesforce_s3_file_viewer_invoke.arn
}