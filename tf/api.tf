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
    logging_level = "INFO"
    data_trace_enabled = true
    throttling_burst_limit = 100
    throttling_rate_limit  = 50
  }

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_gw_logs.arn
    format = jsonencode({
      requestId       = "$context.requestId"
      sourceIp        = "$context.identity.sourceIp"
      requestTime     = "$context.requestTime"
      protocol        = "$context.protocol"
      httpMethod      = "$context.httpMethod"
      resourcePath    = "$context.resourcePath"
      routeKey        = "$context.routeKey"
      status          = "$context.status"
      responseLength  = "$context.responseLength"
      integrationError = "$context.integrationErrorMessage"
    })
  }
}

resource "aws_cloudwatch_log_group" "api_gw_logs" {
  name = "/aws/api_gw/proofing"
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
