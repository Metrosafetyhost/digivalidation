resource "aws_s3_bucket" "metrosafetysandboxfiles" {
  bucket = "metrosafetysandboxfiles"
}

resource "aws_iam_user" "salesforce_prep_s3" {
  name = "salesforce-prep-s3"
}

resource "aws_iam_access_key" "salesforce_prep_s3" {
  user = aws_iam_user.salesforce_prep_s3.name
}

data "aws_iam_policy_document" "salesforce_prep_s3_policy" {
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.metrosafetysandboxfiles.arn]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:HeadObject"
    ]
    resources = ["${aws_s3_bucket.metrosafetysandboxfiles.arn}/*"]
  }
}

resource "aws_iam_policy" "salesforce_prep_s3_policy" {
  name   = "SalesforcePrepS3Policy"
  policy = data.aws_iam_policy_document.salesforce_prep_s3_policy.json
}

resource "aws_iam_user_policy_attachment" "salesforce_prep_s3_policy" {
  user       = aws_iam_user.salesforce_prep_s3.name
  policy_arn = aws_iam_policy.salesforce_prep_s3_policy.arn
}

output "salesforce_prep_s3_access_key_id" {
  value = aws_iam_access_key.salesforce_prep_s3.id
}

output "salesforce_prep_s3_secret_access_key" {
  value     = aws_iam_access_key.salesforce_prep_s3.secret
  sensitive = true
}

resource "aws_s3_bucket" "metrosafetysandboximages" {
  bucket = "metrosafetysandboximages"
}

resource "aws_s3_bucket_public_access_block" "metrosafetysandboximages" {
  bucket = aws_s3_bucket.metrosafetysandboximages.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}