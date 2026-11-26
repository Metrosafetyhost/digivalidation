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
      },
      {
        Sid    = "DenyWorkOrderVisibilityManagement"
        Effect = "Deny"

        Action = [
          "execute-api:Invoke"
        ]

        Resource = [
          "arn:aws:execute-api:eu-west-2:${data.aws_caller_identity.current.account_id}:${aws_apigatewayv2_api.lambda_api.id}/prod/GET/files/workorders/*/visibility",
          "arn:aws:execute-api:eu-west-2:${data.aws_caller_identity.current.account_id}:${aws_apigatewayv2_api.lambda_api.id}/prod/PUT/files/workorders/*/visibility"
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "salesforce_s3_file_viewer" {
  user       = aws_iam_user.salesforce_s3_file_viewer.name
  policy_arn = aws_iam_policy.salesforce_s3_file_viewer_invoke.arn
}

resource "aws_iam_user" "salesforce_s3_visibility_admin" {
  name = "salesforce-s3-visibility-admin"
}

resource "aws_iam_policy" "salesforce_s3_visibility_admin_invoke" {
  name        = "salesforce-s3-visibility-admin-invoke"
  description = "Allows Salesforce visibility administrators to read and update Work Order S3 visibility tags"

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Effect = "Allow"

        Action = [
          "execute-api:Invoke"
        ]

        Resource = [
          "arn:aws:execute-api:eu-west-2:${data.aws_caller_identity.current.account_id}:${aws_apigatewayv2_api.lambda_api.id}/prod/GET/files/workorders/*/visibility",
          "arn:aws:execute-api:eu-west-2:${data.aws_caller_identity.current.account_id}:${aws_apigatewayv2_api.lambda_api.id}/prod/PUT/files/workorders/*/visibility"
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "salesforce_s3_visibility_admin" {
  user       = aws_iam_user.salesforce_s3_visibility_admin.name
  policy_arn = aws_iam_policy.salesforce_s3_visibility_admin_invoke.arn
}
