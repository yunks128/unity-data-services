resource "aws_api_gateway_resource" "misc_catalog_list_resource" {
  rest_api_id = data.aws_api_gateway_rest_api.rest_api.id
  parent_id   = aws_api_gateway_resource.misc_base_resource.id
  path_part   = "catalog_list"
}

resource "aws_api_gateway_method" "misc_catalog_list_method" {
  rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
  resource_id   = aws_api_gateway_resource.misc_catalog_list_resource.id
  http_method   = "GET"
  authorization = "NONE"
  request_parameters = {
    "method.request.path.proxy" = true
  }
}

resource "aws_api_gateway_method_response" "misc_catalog_list_method_response" {
    rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
    resource_id   = aws_api_gateway_resource.misc_catalog_list_resource.id
    http_method   = aws_api_gateway_method.misc_catalog_list_method.http_method
    status_code   = 200
    response_models = {
        "application/json" = "Empty"
    }
    response_parameters = {
        "method.response.header.Access-Control-Allow-Origin" = true
    }
    depends_on = ["aws_api_gateway_method.misc_catalog_list_method"]
}

resource "aws_api_gateway_integration" "misc_catalog_list_lambda_integration" {
  rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
  resource_id          = aws_api_gateway_resource.misc_catalog_list_resource.id
  http_method          = aws_api_gateway_method.misc_catalog_list_method.http_method
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
