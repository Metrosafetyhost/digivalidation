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
}

#perms
resource "aws_lambda_permission" "apigw_lambda" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = bedrock-lambda-salesforce_input
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_apigatewayv2_api.lambda_api.execution_arn}/*/*"
}

# output "api_gateway_url" {
#   value = "${aws_apigatewayv2_api.lambda_api.api_endpoint}/${aws_apigatewayv2_stage.lambda_stage.name}"
# }
