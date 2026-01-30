resource "aws_apigatewayv2_api" "lambda_api" {
  name          = "ProofingLambdaAPI"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "lambda_integration" {
  api_id           = aws_apigatewayv2_api.lambda_api.id
  integration_type = "AWS_PROXY"
  integration_uri  = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-salesforce_input"
}

resource "aws_apigatewayv2_route" "proofing_route" {
  api_id    = aws_apigatewayv2_api.lambda_api.id
  route_key = "POST /proof_html"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

resource "aws_apigatewayv2_stage" "lambda_stage" {
  api_id      = aws_apigatewayv2_api.lambda_api.id
  name        = "prod"
  auto_deploy = true

  default_route_settings {
    logging_level          = "INFO"
    data_trace_enabled     = true
    throttling_burst_limit = 100
    throttling_rate_limit  = 50
  }

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_gw_logs.arn
    format = jsonencode({
      requestId        = "$context.requestId"
      sourceIp         = "$context.identity.sourceIp"
      requestTime      = "$context.requestTime"
      protocol         = "$context.protocol"
      httpMethod       = "$context.httpMethod"
      resourcePath     = "$context.resourcePath"
      routeKey         = "$context.routeKey"
      status           = "$context.status"
      responseLength   = "$context.responseLength"
      integrationError = "$context.integrationErrorMessage"
    })
  }
}

resource "aws_cloudwatch_log_group" "api_gw_logs" {
  name              = "/aws/api_gw/proofing"
  retention_in_days = 7
}

#perms
resource "aws_lambda_permission" "apigw_lambda" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = "bedrock-lambda-salesforce_input"
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.lambda_api.execution_arn}/*/*"
}

# Integration for categorisation lambda
resource "aws_apigatewayv2_integration" "categorisation_integration" {
  api_id           = aws_apigatewayv2_api.lambda_api.id
  integration_type = "AWS_PROXY"
  integration_uri  = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-categorisation"
}

# Route for categorisation endpoint
resource "aws_apigatewayv2_route" "categorisation_route" {
  api_id    = aws_apigatewayv2_api.lambda_api.id
  route_key = "POST /categorisation"
  target    = "integrations/${aws_apigatewayv2_integration.categorisation_integration.id}"
}

# Permission to allow API Gateway to invoke your categorisation lambda
resource "aws_lambda_permission" "apigw_lambda_categorisation" {
  statement_id  = "AllowExecutionFromAPIGatewayCategorisation"
  action        = "lambda:InvokeFunction"
  function_name = "bedrock-lambda-categorisation"
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.lambda_api.execution_arn}/*/*"
}

# 1) Integration for your new DigiValidation Lambda
resource "aws_apigatewayv2_integration" "digivalidation_integration" {
  api_id           = aws_apigatewayv2_api.lambda_api.id
  integration_type = "AWS_PROXY"
  integration_uri  = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-digival"
  # ← update to your actual function ARN
}

# 2) Route for POST /digivalidation
resource "aws_apigatewayv2_route" "digivalidation_route" {
  api_id    = aws_apigatewayv2_api.lambda_api.id
  route_key = "POST /digivalidation"
  target    = "integrations/${aws_apigatewayv2_integration.digivalidation_integration.id}"
}

# 3) Permission to allow API Gateway to invoke the DigiValidation Lambda
resource "aws_lambda_permission" "apigw_lambda_digivalidation" {
  statement_id  = "AllowExecutionFromAPIGatewayDigivalidation"
  action        = "lambda:InvokeFunction"
  function_name = "bedrock-lambda-digival"
  principal  = "apigateway.amazonaws.com"
  source_arn = "${aws_apigatewayv2_api.lambda_api.execution_arn}/*/*"
}

# Integration for Asset Categorisation Lambda
resource "aws_apigatewayv2_integration" "asset_categorisation_integration" {
  api_id           = aws_apigatewayv2_api.lambda_api.id
  integration_type = "AWS_PROXY"
  integration_uri  = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-asset_categorisation"
}

# Route for POST /asset_categorisation
resource "aws_apigatewayv2_route" "asset_categorisation_route" {
  api_id    = aws_apigatewayv2_api.lambda_api.id
  route_key = "POST /asset_categorisation"
  target    = "integrations/${aws_apigatewayv2_integration.asset_categorisation_integration.id}"
}

# Permission to allow API Gateway to invoke the Asset Categorisation Lambda
resource "aws_lambda_permission" "apigw_lambda_asset_categorisation" {
  statement_id  = "AllowExecutionFromAPIGatewayAssetCategorisation"
  action        = "lambda:InvokeFunction"
  function_name = "bedrock-lambda-asset_categorisation"
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.lambda_api.execution_arn}/*/*"
}

#Integration for Water Risk Case Ingest Lambda
resource "aws_apigatewayv2_integration" "water_risk_case_ingest_integration" {
  api_id           = aws_apigatewayv2_api.lambda_api.id
  integration_type = "AWS_PROXY"
  integration_uri  = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-waterRiskCaseIngest"
}

# Route for POST /water_risk_case_ingest
resource "aws_apigatewayv2_route" "water_risk_case_ingest_route" {
  api_id    = aws_apigatewayv2_api.lambda_api.id
  route_key = "POST /water_risk_case_ingest"
  target    = "integrations/${aws_apigatewayv2_integration.water_risk_case_ingest_integration.id}"
}

# Permission to allow API Gateway to invoke the Water Risk Case Ingest Lambda
resource "aws_lambda_permission" "apigw_lambda_water_risk_case_ingest" {
  statement_id  = "AllowExecutionFromAPIGatewayWaterRiskCaseIngest"
  action        = "lambda:InvokeFunction"
  function_name = "bedrock-lambda-waterRiskCaseIngest"
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.lambda_api.execution_arn}/*/*"
}

data "aws_lambda_function" "pdfqa_api" {
  function_name = "bedrock-lambda-pdfqa_api"
}

resource "aws_apigatewayv2_integration" "pdfqa_api_integration" {
  api_id                 = aws_apigatewayv2_api.lambda_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = data.aws_lambda_function.pdfqa_api.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "dewrra_start_route" {
  api_id    = aws_apigatewayv2_api.lambda_api.id
  route_key = "POST /dewrra/start"
  target    = "integrations/${aws_apigatewayv2_integration.pdfqa_api_integration.id}"
}

resource "aws_apigatewayv2_route" "dewrra_status_route" {
  api_id    = aws_apigatewayv2_api.lambda_api.id
  route_key = "GET /dewrra/status/{jobId}"
  target    = "integrations/${aws_apigatewayv2_integration.pdfqa_api_integration.id}"
}

resource "aws_apigatewayv2_route" "dewrra_results_route" {
  api_id    = aws_apigatewayv2_api.lambda_api.id
  route_key = "GET /dewrra/results/{jobId}"
  target    = "integrations/${aws_apigatewayv2_integration.pdfqa_api_integration.id}"
}

resource "aws_lambda_permission" "apigw_lambda_pdfqa_api" {
  statement_id  = "AllowExecutionFromAPIGatewayPdfQaApi"
  action        = "lambda:InvokeFunction"
  function_name = data.aws_lambda_function.pdfqa_api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.lambda_api.execution_arn}/*/*"
}

