# Select the correct IAM role: override if provided, otherwise use the default role
locals {
  lambda_map = zipmap(var.lambda_names, var.lambda_file_names)

  # Normalize external role ARN: null/empty/whitespace -> ""
  external_role_arn = try(trimspace(var.lambda_role_arn), "")

  # Prefer external role when provided; else module-created role
  effective_lambda_roles = {
    for lambda in var.lambda_names :
    lambda => (length(local.external_role_arn) > 0
      ? local.external_role_arn
      : aws_iam_role.lambda[lambda].arn
    )
  }
}

locals {
  event_sources_sns   = length(local.external_role_arn) == 0 ? { for k, v in var.lambda_event_sources : k => v if v.source_type == "sns" } : {}
  event_sources_apigw = length(local.external_role_arn) == 0 ? { for k, v in var.lambda_event_sources : k => v if v.source_type == "apigateway" } : {}
  event_sources_sqs   = length(local.external_role_arn) == 0 ? { for k, v in var.lambda_event_sources : k => v if v.source_type == "sqs" } : {}
  attach_logs_roles   = length(local.external_role_arn) == 0 ? local.lambda_map : {}
}

locals {
  # defaults (can be empty strings)
  _def_hdl = trimspace(try(var.handler, ""))
  _def_rt  = trimspace(try(var.runtime, ""))

  # safe per-lambda overrides (always end up as strings, maybe "")
  handler_override = {
    for k in keys(local.lambda_map) :
    k => trimspace(
      try(lookup(lookup(var.lambda_config, k, {}), "handler", ""), "")
    )
  }
  runtime_override = {
    for k in keys(local.lambda_map) :
    k => trimspace(
      try(lookup(lookup(var.lambda_config, k, {}), "runtime", ""), "")
    )
  }

  # final resolved pieces (never unknown/empty)
  resolved_handler_name = {
    for k in keys(local.lambda_map) :
    k => coalesce(
      length(local.handler_override[k]) > 0 ? local.handler_override[k] : null,
      length(local._def_hdl) > 0 ? local._def_hdl : null,
      "process"
    )
  }
  resolved_runtime = {
    for k in keys(local.lambda_map) :
    k => coalesce(
      length(local.runtime_override[k]) > 0 ? local.runtime_override[k] : null,
      length(local._def_rt) > 0 ? local._def_rt : null,
      "python3.12"
    )
  }

  computed_handler = {
    for k in keys(local.lambda_map) :
    k => "${k}.${local.resolved_handler_name[k]}"
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
  for_each           = length(local.external_role_arn) == 0 ? local.lambda_map : {}
  assume_role_policy = data.aws_iam_policy_document.lambda.json
  name               = "${var.namespace}-${each.key}"

  tags = merge(local.common_tags, {
    git_file = "modules/lambdas-zip/main.tf"
  })
}

# Default Execution Role Attachment (Basic Lambda Execution)
resource "aws_iam_role_policy_attachment" "lambda_execute" {
  for_each   = length(local.external_role_arn) == 0 ? local.lambda_map : {}
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
  package_type  = "Zip"

  # TEMP: force known-good values so provider can’t claim they’re missing
  handler = local.computed_handler[each.key]
  runtime = local.resolved_runtime[each.key]

  # keep the rest the same…
  role      = local.effective_lambda_roles[each.key]
  s3_bucket = var.s3_zip_bucket
  s3_key    = aws_s3_object.lambda_zip[each.key].key


  architectures = [coalesce(try(var.lambda_config[each.key].arch, null), var.arch, "arm64")]

  layers = coalesce(
    try(length(var.lambda_config[each.key].lambda_layers) > 0 ? var.lambda_config[each.key].lambda_layers : null, null),
    length(var.lambda_layer_arns) > 0 ? var.lambda_layer_arns : null,
    var.lambda_layer_arn != "" ? [var.lambda_layer_arn] : null,
    []
  )

  memory_size   = try(var.lambda_config[each.key].memory_size, 512)
  timeout       = try(var.lambda_config[each.key].timeout, 240)

  # (postconditions optional; you can remove them)
  # lifecycle {
  #   postcondition {
  #     condition     = length(trimspace(self.handler)) > 0
  #     error_message = "handler is empty for function ${each.key}"
  #   }
  #   postcondition {
  #     condition     = length(trimspace(self.runtime)) > 0
  #     error_message = "runtime is empty for function ${each.key}"
  #   }
  # }

  # ...rest unchanged...

  source_code_hash = aws_s3_object.lambda_zip[each.key].metadata["commit"] == data.external.git.result["sha"] ? (
    var.force_lambda_code_deploy ? aws_s3_object.lambda_zip[each.key].metadata["hash"] : null
  ) : aws_s3_object.lambda_zip[each.key].metadata["hash"]

  environment {
  variables = merge(
    var.default_environment,
    try(var.lambda_config[each.key].lambda_environment, {})
  )
  }

  tags = merge(local.common_tags, {
    git_file = "modules/lambdas-zip/main.tf"
  }
  )
}

output "debug_computed_handler" {
  value = local.computed_handler
}

output "debug_resolved_runtime" {
  value = local.resolved_runtime
}


# output "debug_handlers" {
#   value = {
#     for k in keys(local.lambda_map) :
#     k => format(
#       "%s.%s",
#       k,
#       length(trimspace(try(var.lambda_config[k].handler, ""))) > 0
#         ? trimspace(var.lambda_config[k].handler)
#         : (length(trimspace(var.handler)) > 0 ? trimspace(var.handler) : "process")
#     )
#   }
# }

# output "debug_runtimes" {
#   value = {
#     for k in keys(local.lambda_map) :
#     k => (
#       length(trimspace(try(var.lambda_config[k].runtime, ""))) > 0
#         ? trimspace(var.lambda_config[k].runtime)
#         : (length(trimspace(var.runtime)) > 0 ? trimspace(var.runtime) : "python3.12")
#     )
#   }
# }

# output "debug_handlers"  { value = module.lambdas_zip.debug_handlers }
# output "debug_runtimes"  { value = module.lambdas_zip.debug_runtimes }



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

# Only attach policies to module-created roles when we are creating them
resource "aws_iam_role_policy_attachment" "sns_lambda" {
  for_each   = local.event_sources_sns
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda[each.key].name
}

resource "aws_iam_role_policy_attachment" "api_lambda" {
  for_each   = local.event_sources_apigw
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda[each.key].name
}

resource "aws_iam_role_policy_attachment" "sqs_lambda" {
  for_each   = local.event_sources_sqs
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole"
  role       = aws_iam_role.lambda[each.key].name
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  for_each   = local.attach_logs_roles
  role       = aws_iam_role.lambda[each.key].name
  policy_arn = aws_iam_policy.lambda_logging[each.key].arn
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

# # --- locals: paths & names ---
# locals {
#   layer_name  = "openai-deps"
#   runtimes    = ["python3.11", "python3.12"]   # include the one your Lambda uses
#   build_root  = "${path.module}/.build"
#   dist_root   = "${path.module}/dist"
#   layer_dir   = "${local.build_root}/${local.layer_name}"
#   layer_zip   = "${local.dist_root}/${local.layer_name}.zip"
#   reqs_file   = "${path.module}/layer-requirements.txt"
# }

# # --- build the layer zip locally ---
# resource "null_resource" "build_openai_layer" {
#   triggers = { req_hash = filesha256(local.reqs_file) }

#   provisioner "local-exec" {
#     interpreter = ["/bin/bash", "-lc"]
#     command = <<-EOT
#       set -euo pipefail
#       rm -rf "${local.layer_dir}" "${local.layer_zip}"
#       mkdir -p "${local.layer_dir}/python" "${local.dist_root}"
#       python3 -m pip install --upgrade pip
#       python3 -m pip install -r "${local.reqs_file}" -t "${local.layer_dir}/python"
#       (cd "${local.layer_dir}" && zip -qr "${local.layer_zip}" .)
#       echo "Built layer at ${local.layer_zip}"
#     EOT
#   }
# }

# # --- publish the layer in AWS ---
# resource "aws_lambda_layer_version" "openai" {
#   filename            = local.layer_zip
#   layer_name          = local.layer_name
#   compatible_runtimes = local.runtimes
#   source_code_hash    = filesha256(local.layer_zip)
#   depends_on          = [null_resource.build_openai_layer]
# }