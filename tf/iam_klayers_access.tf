# Allow SSO Admin role to fetch the Klayers Pillow layer
resource "aws_iam_policy" "allow_get_klayers" {
  name   = "AllowGetKlayers"
  path   = "/"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "lambda:GetLayerVersion"
        ],
        Resource = "arn:aws:lambda:eu-west-2:770693421928:layer:Klayers-p313-pillow:1"
      }
    ]
  })
}

# Attach the policy to your SSO Admin role
resource "aws_iam_role_policy_attachment" "attach_allow_get_klayers" {
  role       = "AWSReservedSSO_Admin_5ecf716297da6414"
  policy_arn = aws_iam_policy.allow_get_klayers.arn
}
