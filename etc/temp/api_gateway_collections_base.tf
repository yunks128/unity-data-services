resource "aws_api_gateway_resource" "collections_base_resource" {
  rest_api_id = data.aws_api_gateway_rest_api.rest_api.id
  parent_id   = aws_api_gateway_resource.uds_api_base_resource.id
  path_part   = "collections"
}

resource "aws_api_gateway_method" "collections_base_method" {
  rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
  resource_id   = aws_api_gateway_resource.collections_base_resource.id
  http_method   = "ANY"
  authorization = "NONE"
  request_parameters = {
    "method.request.path.proxy" = true
  }
}

resource "aws_api_gateway_method_response" "collections_base_method_response" {
    rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
    resource_id   = aws_api_gateway_resource.collections_base_resource.id
    http_method   = aws_api_gateway_method.collections_base_method.http_method
    status_code   = 200
    response_models = {
        "application/json" = "Empty"
    }
    response_parameters = {
        "method.response.header.Access-Control-Allow-Origin" = true
    }
    depends_on = ["aws_api_gateway_method.collections_base_method"]
}

resource "aws_api_gateway_integration" "collections_base_lambda_integration" {
  rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
  resource_id          = aws_api_gateway_resource.collections_base_resource.id
  http_method          = aws_api_gateway_method.collections_base_method.http_method
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

resource "aws_api_gateway_method" "collections_base_options_method" {
    rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
    resource_id   = aws_api_gateway_resource.collections_base_resource.id
    http_method   = "OPTIONS"
    authorization = "NONE"
}
resource "aws_api_gateway_method_response" "collections_base_options_200" {
    rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
    resource_id   = aws_api_gateway_resource.collections_base_resource.id
    http_method   = aws_api_gateway_method.collections_base_options_method.http_method
    status_code   = 200
    response_models = {
        "application/json" = "Empty"
    }
    response_parameters = {
        "method.response.header.Access-Control-Allow-Headers" = true
        "method.response.header.Access-Control-Allow-Methods" = true
        "method.response.header.Access-Control-Allow-Origin" = true
        "method.response.header.Access-Control-Expose-Headers" = true
        "method.response.header.Access-Control-Max-Age" = true
    }
    depends_on = ["aws_api_gateway_method.collections_base_options_method"]
}
resource "aws_api_gateway_integration" "collections_base_options_integration" {
    rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
    resource_id   = aws_api_gateway_resource.collections_base_resource.id
    http_method   = aws_api_gateway_method.collections_base_options_method.http_method
        type          = "MOCK"
    request_templates = {
      "application/json" = jsonencode(
        {
          statusCode = 200
        })
    }
    depends_on = ["aws_api_gateway_method.collections_base_options_method"]
}
resource "aws_api_gateway_integration_response" "collections_base_options_integration_response" {
    rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
    resource_id   = aws_api_gateway_resource.collections_base_resource.id
    http_method   = aws_api_gateway_method.collections_base_options_method.http_method
    status_code   = aws_api_gateway_method_response.collections_base_options_200.status_code
    response_parameters = {
        "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
        "method.response.header.Access-Control-Allow-Methods" = "'DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT'",
        "method.response.header.Access-Control-Allow-Origin" = "'*'"
        "method.response.header.Access-Control-Expose-Headers" = "'Access-Control-Allow-Methods,Access-Control-Allow-Origin,Access-Control-Expose-Headers,Access-Control-Max-Age'"
        "method.response.header.Access-Control-Max-Age" = "'300'"
    }
    depends_on = ["aws_api_gateway_method_response.collections_base_options_200"]
}