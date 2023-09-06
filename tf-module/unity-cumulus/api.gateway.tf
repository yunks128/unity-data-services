data "aws_api_gateway_rest_api" "base_uds_dapa_rest_api" {
  name = "Unity API Gateway 1"  # TODO. come from var
}

data "aws_api_gateway_resource" "base_uds_dapa_collections_resource" {
  rest_api_id = data.aws_api_gateway_rest_api.base_uds_dapa_rest_api.id
  path        = "/am-uds-dapa/collections"
}


resource "aws_api_gateway_method" "dapa_collection_get_method" {
  rest_api_id   = data.aws_api_gateway_rest_api.base_uds_dapa_rest_api.id
  resource_id   = data.aws_api_gateway_resource.base_uds_dapa_collections_resource.id
  http_method   = "GET"
  authorization = "CUSTOM"
  authorizer_id = "lkosve"  # TODO. come from var
}

resource "aws_api_gateway_integration" "dapa_collection_get_integration" {
  rest_api_id = data.aws_api_gateway_rest_api.base_uds_dapa_rest_api.id
  resource_id = data.aws_api_gateway_resource.base_uds_dapa_collections_resource.id
  http_method = aws_api_gateway_method.dapa_collection_get_method.http_method
  integration_http_method = "GET"
  type                    = "AWS_PROXY"
  uri = aws_lambda_function.cumulus_collections_dapa.invoke_arn
}

# Lambda
resource "aws_lambda_permission" "dapa_collection_get_apigw_lambda" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cumulus_collections_dapa.function_name
  principal     = "apigateway.amazonaws.com"

  # More: http://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-control-access-using-iam-policies-to-invoke-api.html
  source_arn = "arn:aws:execute-api:${var.aws_region}:884500545225:${data.aws_api_gateway_rest_api.base_uds_dapa_rest_api.id}/*/${aws_api_gateway_method.dapa_collection_get_method.http_method}${data.aws_api_gateway_resource.base_uds_dapa_collections_resource.path}"
}