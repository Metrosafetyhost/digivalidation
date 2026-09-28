resource "aws_iam_policy" "s3_visibility_tagger_tagging" {
  name        = "${var.namespace}-${var.env}-s3-visibility-tagger-tagging"
  description = "Allow the visibility tagger to read and write tags on production files"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "TagBuildingAndWorkOrderFiles"
      Effect = "Allow"
      Action = [
        "s3:GetObjectTagging",
        "s3:PutObjectTagging"
      ]
      Resource = [
        "arn:aws:s3:::metrosafetyprodfiles/Buildings/*",
        "arn:aws:s3:::metrosafetyprodfiles/WorkOrders/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "s3_visibility_tagger_tagging" {
  role       = "${var.namespace}-s3_visibility_tagger"
  policy_arn = aws_iam_policy.s3_visibility_tagger_tagging.arn

  depends_on = [module.lambdas_zip]
}

resource "aws_lambda_permission" "allow_files_bucket_invoke_s3_visibility_tagger" {
  statement_id   = "AllowExecutionFromS3MetroSafetyProdFiles"
  action         = "lambda:InvokeFunction"
  function_name  = module.lambdas_zip.lambda_arns["s3_visibility_tagger"]
  principal      = "s3.amazonaws.com"
  source_arn     = "arn:aws:s3:::metrosafetyprodfiles"
  source_account = local.this_account
}

resource "aws_s3_bucket_notification" "files_visibility_tagger" {
  bucket = "metrosafetyprodfiles"

  lambda_function {
    lambda_function_arn = module.lambdas_zip.lambda_arns["s3_visibility_tagger"]
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "Buildings/"
  }

  lambda_function {
    lambda_function_arn = module.lambdas_zip.lambda_arns["s3_visibility_tagger"]
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "WorkOrders/"
  }

  depends_on = [aws_lambda_permission.allow_files_bucket_invoke_s3_visibility_tagger]
}
