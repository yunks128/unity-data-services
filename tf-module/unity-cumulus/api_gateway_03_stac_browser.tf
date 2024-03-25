resource "aws_api_gateway_resource" "stac_browser_resource" {
  rest_api_id = data.aws_api_gateway_rest_api.rest_api.id
  parent_id   = aws_api_gateway_resource.uds_api_base_resource.id
  path_part   = "stac_browser"
}

resource "aws_api_gateway_method" "stac_browser_method" {
  rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
  resource_id   = aws_api_gateway_resource.stac_browser_resource.id
  http_method   = "GET"
  authorization = "NONE"
  request_parameters = {
    "method.request.path.proxy" = true
  }
}

resource "aws_api_gateway_method_response" "stac_browser_method_response" {
    rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
    resource_id   = aws_api_gateway_resource.stac_browser_resource.id
    http_method   = aws_api_gateway_method.stac_browser_method.http_method
    status_code   = 200
    response_models = {
        "application/json" = "Empty"
    }
    response_parameters = {
        "method.response.header.Access-Control-Allow-Origin" = true
    }
    depends_on = ["aws_api_gateway_method.stac_browser_method"]
}

resource "aws_api_gateway_integration" "stac_browser_lambda_integration" {
  rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
  resource_id          = aws_api_gateway_resource.stac_browser_resource.id
  http_method          = aws_api_gateway_method.stac_browser_method.http_method
  type                 = "AWS_PROXY"
  uri = aws_lambda_function.uds_api_1.invoke_arn
  integration_http_method = "POST"

#  cache_key_parameters = ["method.request.path.proxy"]

  timeout_milliseconds = 29000
#  request_parameters = {
#    "integration.request.path.proxy" = "method.request.path.proxy"
#  }
}

##########################################################################################################################
