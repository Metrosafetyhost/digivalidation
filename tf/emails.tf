# 1. (Optional) Verify your "From" address in SES
#    — only needed if you haven’t already done this in Terraform:
resource "aws_ses_email_identity" "sender" {
  email = "luke.gasson@metrosafety.co.uk"
}

resource "aws_ses_email_identity" "recipient" {
  email = "luke.gasson@metrosafety.co.uk"
}

# 2. Look up the Lambda’s execution role by name
data "aws_iam_role" "salesforce_input" {
  name = "bedrock-lambda-salesforce_input"
}

# 3. Define a policy that allows SendEmail (and SendRawEmail if you need it)
resource "aws_iam_policy" "ses_send_email" {
  name        = "bedrock-lambda-ses-policy"
  description = "Allow only sending from our verified address"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "AllowSendEmailFromMyAddress",
      Effect   = "Allow",
      Action   = ["ses:SendEmail","ses:SendRawEmail"],
      Resource = "arn:aws:ses:eu-west-2:123456789012:identity/luke.gasson@metrosafety.co.uk"
    }]
  })
}


# 4. Attach that policy to the Lambda role
resource "aws_iam_role_policy_attachment" "salesforce_input_ses" {
  role       = data.aws_iam_role.salesforce_input.name
  policy_arn = aws_iam_policy.ses_send_email.arn
}
