# Large-file hand-off consumer (Salesforce -> S3 -> NEILON S3-Link).
# Deployment does not start processing: the rule and worker are both disabled.
resource "aws_iam_role_policy" "s3_large_file_worker" {
  name = "S3LargeFileWorkerAccess"
  role = "${var.namespace}-s3_large_file_worker"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadSalesforceOAuthSecret"
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = aws_secretsmanager_secret.sf_oauth_secret.arn
      },
      {
        Sid      = "CheckDestinationBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = "arn:aws:s3:::metrosafetyprodfiles"
      },
      {
        Sid    = "TransferSupportedEntityFiles"
        Effect = "Allow"
        Action = [
          "s3:GetObject", # HeadObject uses GetObject permissions.
          "s3:PutObject"
        ]
        Resource = [
          "arn:aws:s3:::metrosafetyprodfiles/WorkOrders/*",
          "arn:aws:s3:::metrosafetyprodfiles/Opportunities/*",
          "arn:aws:s3:::metrosafetyprodfiles/Buildings/*"
        ]
      }
    ]
  })

  depends_on = [module.lambdas_zip]
}

# No automatic invocations until the integration has been verified and approved.
resource "aws_cloudwatch_event_rule" "s3_large_file_worker" {
  name                = "${var.namespace}-s3-large-file-worker"
  description         = "Poll Salesforce for pending large-file hand-offs"
  schedule_expression = "rate(10 minutes)"
  state               = "DISABLED"
}

resource "aws_lambda_permission" "s3_large_file_worker_schedule" {
  statement_id  = "AllowLargeFileWorkerSchedule"
  action        = "lambda:InvokeFunction"
  function_name = module.lambdas_zip.lambda_function_arns["s3_large_file_worker"]
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.s3_large_file_worker.arn
}

resource "aws_cloudwatch_event_target" "s3_large_file_worker" {
  rule      = aws_cloudwatch_event_rule.s3_large_file_worker.name
  target_id = "S3LargeFileWorker"
  arn       = module.lambdas_zip.lambda_function_arns["s3_large_file_worker"]

  depends_on = [aws_lambda_permission.s3_large_file_worker_schedule]
}

# UK end-of-day schedule. The legacy 10-minute EventBridge rule above stays disabled.
# AWS EventBridge Scheduler observes Europe/London daylight-saving transitions.
resource "aws_iam_role" "s3_large_file_worker_daily_scheduler" {
  name = "${var.namespace}-s3-large-file-worker-daily-scheduler"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = {
        Service = "scheduler.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "s3_large_file_worker_daily_scheduler" {
  name = "InvokeS3LargeFileWorker"
  role = aws_iam_role.s3_large_file_worker_daily_scheduler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["lambda:InvokeFunction"]
      Resource = module.lambdas_zip.lambda_function_arns["s3_large_file_worker"]
    }]
  })
}

resource "aws_scheduler_schedule" "s3_large_file_worker_daily" {
  name                         = "${var.namespace}-s3-large-file-worker-daily"
  description                  = "Process one Pending Salesforce large-file request each UK evening"
  schedule_expression          = "cron(0 18 * * ? *)"
  schedule_expression_timezone = "Europe/London"
  state                        = "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = module.lambdas_zip.lambda_function_arns["s3_large_file_worker"]
    role_arn = aws_iam_role.s3_large_file_worker_daily_scheduler.arn
    input = jsonencode({
      action = "process_pending"
      source = "metro-safety-daily-scheduler"
    })

    retry_policy {
      maximum_retry_attempts = 0
    }
  }

  depends_on = [aws_iam_role_policy.s3_large_file_worker_daily_scheduler]
}
