############################################
# dewrra_jobs (DynamoDB) + SQS + event mapping
############################################

resource "aws_dynamodb_table" "dewrra_jobs" {
  name         = "dewrra_jobs"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "jobId"

  attribute {
    name = "jobId"
    type = "S"
  }

  tags = {
    Name        = "dewrra_jobs"
    Environment = var.env
  }
}

resource "aws_sqs_queue" "dewrra_jobs_dlq" {
  name = "dewrra_jobs_dlq"
}

resource "aws_sqs_queue" "dewrra_jobs_queue" {
  name = "dewrra_jobs_queue"
  
  visibility_timeout_seconds = 300

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dewrra_jobs_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_lambda_event_source_mapping" "pdfqa_worker_sqs" {
  event_source_arn = aws_sqs_queue.dewrra_jobs_queue.arn
  function_name    = "bedrock-lambda-pdf_qa"

  batch_size                         = 1
  maximum_batching_window_in_seconds = 0
}

############################################
# pdfqa_api (HTTP API lambda) - minimal IAM
############################################

data "aws_iam_role" "pdfqa_api_role" {
  name = "bedrock-lambda-pdfqa_api"
}

data "aws_iam_policy_document" "pdfqa_api_policy" {
  statement {
    sid     = "DdbPutGetJobs"
    effect  = "Allow"
    actions = ["dynamodb:PutItem", "dynamodb:GetItem"]
    resources = [aws_dynamodb_table.dewrra_jobs.arn]
  }

  statement {
    sid     = "SqsSendJob"
    effect  = "Allow"
    actions = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dewrra_jobs_queue.arn]
  }

  # API reads results json from WorkOrders/<id>/results/<jobId>.json
  statement {
    sid     = "S3ReadWorkOrderResults"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = ["arn:aws:s3:::metrosafetyprodfiles/WorkOrders/*/results/*"]
  }
}

resource "aws_iam_policy" "pdfqa_api_policy" {
  name   = "pdfqa-api-ddb-sqs-s3-results-read"
  policy = data.aws_iam_policy_document.pdfqa_api_policy.json
}

resource "aws_iam_role_policy_attachment" "pdfqa_api_attach" {
  role       = data.aws_iam_role.pdfqa_api_role.name
  policy_arn = aws_iam_policy.pdfqa_api_policy.arn
}

############################################
# pdf_qa worker (SQS consumer lambda) - ONLY missing perms
############################################

data "aws_iam_role" "pdfqa_worker_role" {
  name = "bedrock-lambda-pdf_qa"
}

data "aws_iam_policy_document" "pdfqa_worker_policy" {
  # Worker reads job record + updates status/result pointers
  statement {
    sid     = "DdbGetUpdateJobs"
    effect  = "Allow"
    actions = ["dynamodb:GetItem", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.dewrra_jobs.arn]
  }

  # Worker writes results JSON to WorkOrders/<workOrderId>/results/<jobId>.json
  statement {
    sid     = "S3PutWorkOrderResults"
    effect  = "Allow"
    actions = ["s3:PutObject", "s3:PutObjectTagging"]
    resources = ["arn:aws:s3:::metrosafetyprodfiles/WorkOrders/*/results/*"]
  }

}

resource "aws_iam_policy" "pdfqa_worker_policy" {
  name   = "pdfqa-worker-ddb-results-secrets"
  policy = data.aws_iam_policy_document.pdfqa_worker_policy.json
}

resource "aws_iam_role_policy_attachment" "pdfqa_worker_attach" {
  role       = data.aws_iam_role.pdfqa_worker_role.name
  policy_arn = aws_iam_policy.pdfqa_worker_policy.arn
}

# Required for SQS event source mapping -> Lambda poll/ack
resource "aws_iam_role_policy_attachment" "pdfqa_worker_sqs_managed" {
  role       = data.aws_iam_role.pdfqa_worker_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole"
}
