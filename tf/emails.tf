# 1. (Optional) Verify your "From" address in SES
resource "aws_ses_email_identity" "sender" {
  email = "luke.gasson@metrosafety.co.uk"
}

resource "aws_ses_email_identity" "recipient" {
  email = "luke.gasson@metrosafety.co.uk"
}

# 2. Look up the Salesforce‐input Lambda role (you already have this)
data "aws_iam_role" "salesforce_input" {
  name = "bedrock-lambda-salesforce_input"
}

# 2b. Look up the checklist_proofing Lambda role 
data "aws_iam_role" "checklist_proofing" {
  name = "bedrock-lambda-checklist_proofing"
}

# 3. Define a policy that allows SendEmail (and SendRawEmail)
resource "aws_iam_policy" "ses_send_email" {
  name        = "bedrock-lambda-ses-policy"
  description = "Allow only sending from our verified address"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "AllowSendEmailFromMyAddress",
      Effect   = "Allow",
      Action   = ["ses:SendEmail","ses:SendRawEmail"],
      Resource = "arn:aws:ses:eu-west-2:${data.aws_caller_identity.current.account_id}:identity/luke.gasson@metrosafety.co.uk"
    }]
  })
}

# 4. Attach that policy to the Salesforce‐input role (you already have this)
resource "aws_iam_role_policy_attachment" "salesforce_input_ses" {
  role       = data.aws_iam_role.salesforce_input.name
  policy_arn = aws_iam_policy.ses_send_email.arn
}

# 4b. Attach the same policy to the checklist_proofing role (new)
resource "aws_iam_role_policy_attachment" "checklist_proofing_ses" {
  role       = data.aws_iam_role.checklist_proofing.name
  policy_arn = aws_iam_policy.ses_send_email.arn
}

