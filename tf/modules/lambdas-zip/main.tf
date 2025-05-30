# Select the correct IAM role: override if provided, otherwise use the default role
locals {
  lambda_map = zipmap(var.lambda_names, var.lambda_file_names)

  effective_lambda_roles = {
    for lambda in var.lambda_names :
    lambda => coalesce(var.lambda_role_arn, aws_iam_role.lambda[lambda].arn)
  }
}

data "external" "git" {
  program = ["git", "log", "--pretty=format:{ \"sha\": \"%H\" }", "-1", "HEAD"]
}

data "aws_iam_policy_document" "lambda" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      identifiers = ["lambda.amazonaws.com"]
      type        = "Service"
    }
  }
}

# IAM Role for each Lambda (if not provided)
resource "aws_iam_role" "lambda" {
  for_each           = var.lambda_role_arn == null ? local.lambda_map : {}
  assume_role_policy = data.aws_iam_policy_document.lambda.json
  name               = "${var.namespace}-${each.key}"

  tags = merge(local.common_tags, {
    git_file = "modules/lambdas-zip/main.tf"
  })
}

# Default Execution Role Attachment (Basic Lambda Execution)
resource "aws_iam_role_policy_attachment" "lambda_execute" {
  for_each   = var.lambda_role_arn == null ? local.lambda_map : {}
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda[each.key].name
}

# Create ZIP and Upload to S3
resource "aws_s3_object" "lambda_zip" {
  for_each = local.lambda_map
  bucket   = var.s3_zip_bucket
  key      = "${var.namespace}-${each.key}.zip"
  source   = abspath("../${var.build_dir}/${each.key}.zip")
  etag     = filemd5(abspath("../${var.build_dir}/${each.key}.zip"))

  metadata = {
    hash        = filebase64sha256(abspath("../${var.build_dir}/${each.key}.zip"))
    lambda-name = "${var.namespace}-${each.key}"
    commit      = try(data.external.git.result["sha"], "null")
  }
}

# Lambda Function Deployment
resource "aws_lambda_function" "lambda" {
  for_each      = local.lambda_map
  function_name = "${var.namespace}-${each.key}"
  handler = "${each.key}.${lookup(var.lambda_config, each.key, {
    handler            = "process"
    memory_size        = 512
    timeout            = 240
    lambda_environment = {}
  }).handler}"
  runtime       = var.runtime
  architectures = [var.arch]
  role          = local.effective_lambda_roles[each.key]
  s3_bucket     = var.s3_zip_bucket
  s3_key        = aws_s3_object.lambda_zip[each.key].key
  layers        = var.lambda_layer_arn != "" ? [var.lambda_layer_arn] : []
  memory_size = lookup(var.lambda_config, each.key, {
    handler            = "process"
    memory_size        = 512
    timeout            = 240
    lambda_environment = {}
  }).memory_size
  timeout = lookup(var.lambda_config, each.key, {
    handler            = "process"
    memory_size        = 512
    timeout            = 240
    lambda_environment = {}
  }).timeout


  source_code_hash = aws_s3_object.lambda_zip[each.key].metadata["commit"] == data.external.git.result["sha"] ? (
    var.force_lambda_code_deploy ? aws_s3_object.lambda_zip[each.key].metadata["hash"] : null
  ) : aws_s3_object.lambda_zip[each.key].metadata["hash"]

  environment {
    variables = merge(var.default_environment, lookup(var.lambda_config, each.key, {
      handler            = "process"
      memory_size        = 512
      timeout            = 240
      lambda_environment = {}
    }).lambda_environment)
  }

  tags = merge(local.common_tags, {
    git_file = "modules/lambdas-zip/main.tf"
  })
}

resource "aws_lambda_permission" "lambda" {
  for_each = { for lambda_name, config in var.lambda_event_sources : lambda_name => config if config.source_type != "" }

  action        = "lambda:InvokeFunction"
  principal     = "${each.value.source_type}.amazonaws.com"
  function_name = aws_lambda_function.lambda[each.key].function_name
  source_arn    = each.value.source_arn
}

resource "aws_lambda_event_source_mapping" "lambda" {
  for_each = { for lambda_name, config in var.lambda_event_sources : lambda_name => config if config.source_type == "sqs" }

  batch_size       = 1
  function_name    = aws_lambda_function.lambda[each.key].arn
  event_source_arn = each.value.source_arn
  enabled          = true
}

resource "aws_sns_topic_subscription" "lambda" {
  for_each = { for lambda_name, config in var.lambda_event_sources : lambda_name => config if config.source_type == "sns" }

  topic_arn = each.value.source_arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.lambda[each.key].arn
}

resource "aws_iam_role_policy_attachment" "sns_lambda" {
  for_each = { for lambda_name, config in var.lambda_event_sources : lambda_name => config if config.source_type == "sns" }

  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda[each.key].name
}

resource "aws_iam_role_policy_attachment" "api_lambda" {
  for_each = { for lambda_name, config in var.lambda_event_sources : lambda_name => config if config.source_type == "apigateway" }

  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda[each.key].name
}

resource "aws_iam_role_policy_attachment" "sqs_lambda" {
  for_each = { for lambda_name, config in var.lambda_event_sources : lambda_name => config if config.source_type == "sqs" }

  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole"
  role       = aws_iam_role.lambda[each.key].name
}

# # Source Event Mapping Permissions to Source ARN
# resource "aws_lambda_permission" "lambda" {
#   for_each = {
#     for lambda_name, sources in var.lambda_event_sources :
#     "${lambda_name}.${keys(sources)[0]}" => sources[keys(sources)[0]]
#     if sources[keys(sources)[0]].source_arn != ""
#   }
#   action    = "lambda:InvokeFunction"
#   principal = "${each.value.type}.amazonaws.com"
#   function_name = lookup(aws_lambda_function.lambda, split(".", each.key)[0], {
#     function_name = ""
#   }).function_name
#   source_arn = each.value.source_arn
# }

# resource "aws_sns_topic_subscription" "sqs" {
#   for_each = {
#     for lambda_name, sources in var.lambda_event_sources :
#     "${lambda_name}.${keys(sources)[0]}" => sources[keys(sources)[0]]
#     if sources[keys(sources)[0]].type == "sqs"
#   }
#   topic_arn = each.value.source_arn
#   protocol  = "lambda"
#   endpoint  = aws_lambda_function.lambda[each.key].arn
# }

# resource "aws_iam_role_policy_attachment" "sns_lambda" {
#   for_each = {
#     for lambda_name, sources in var.lambda_event_sources :
#     "${lambda_name}.${keys(sources)[0]}" => sources[keys(sources)[0]]
#     if sources[keys(sources)[0]].type == "sns"
#   }
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
#   role       = aws_iam_role.lambda[split(".", each.key)[0]].name
# }

# resource "aws_iam_role_policy_attachment" "api_lambda" {
#   for_each = {
#     for lambda_name, sources in var.lambda_event_sources :
#     "${lambda_name}.${keys(sources)[0]}" => sources[keys(sources)[0]]
#     if sources[keys(sources)[0]].type == "apigateway"
#   }
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
#   role       = aws_iam_role.lambda[split(".", each.key)[0]].name
# }

# resource "aws_iam_role_policy_attachment" "sqs_lambda" {
#   for_each = {
#     for lambda_name, sources in var.lambda_event_sources :
#     "${lambda_name}.${keys(sources)[0]}" => sources[keys(sources)[0]]
#     if sources[keys(sources)[0]].type == "sqs"
#   }
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole"
#   role       = aws_iam_role.lambda[split(".", each.key)[0]].name
# }

# CloudWatch Log Groups for Each Lambda
resource "aws_cloudwatch_log_group" "lambda_logging" {
  for_each          = local.lambda_map
  name              = "/aws/lambda/${var.namespace}-${each.key}"
  retention_in_days = var.log_retention
}

data "aws_iam_policy_document" "lambda_logging" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

# IAM Role Policy for Logging
resource "aws_iam_policy" "lambda_logging" {
  for_each    = local.lambda_map
  name        = "${var.namespace}-${each.key}-logging"
  description = "IAM policy for CloudWatch logging from a Lambda"
  policy      = data.aws_iam_policy_document.lambda_logging.json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  for_each   = local.lambda_map
  role       = aws_iam_role.lambda[each.key].name
  policy_arn = aws_iam_policy.lambda_logging[each.key].arn
}
