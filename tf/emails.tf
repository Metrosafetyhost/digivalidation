# 1. Verify your "From" address in SES
resource "aws_ses_email_identity" "sender" {
  email = "luke.gasson@metrosafety.co.uk"
}

resource "aws_ses_email_identity" "recipient_luke" {
  email = "luke.gasson@metrosafety.co.uk"
}

resource "aws_ses_email_identity" "recipient_peter" {
  email = "peter.taylor@metrosafety.co.uk"
}

resource "aws_ses_email_identity" "recipient_cristian" {
  email = "cristian.carabus@metrosafety.co.uk"
}

resource "aws_ses_email_identity" "metroit" {
  email = "metroit@metrosafety.co.uk"
}

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
      Resource = "arn:aws:ses:eu-west-2:${data.aws_caller_identity.current.account_id}:identity/*"
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

# look up the FRA-proofing Lambda role
data "aws_iam_role" "fra_proofing" {
  name = "bedrock-lambda-fra_checklist_proofing"
}

# attach SES send policy to it
resource "aws_iam_role_policy_attachment" "fra_proofing_ses" {
  role       = data.aws_iam_role.fra_proofing.name
  policy_arn = aws_iam_policy.ses_send_email.arn
}

# look up the HSA-proofing Lambda role
data "aws_iam_role" "hsa_proofing" {
  name = "bedrock-lambda-hsa_checklist_proofing"
}

# attach SES send policy to it
resource "aws_iam_role_policy_attachment" "hsa_proofing_ses" {
  role       = data.aws_iam_role.hsa_proofing.name
  policy_arn = aws_iam_policy.ses_send_email.arn
}


