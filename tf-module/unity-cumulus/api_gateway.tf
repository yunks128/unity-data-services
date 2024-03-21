data "aws_api_gateway_rest_api" "rest_api" {

  # Name of the REST API to look up. If no REST API is found with this name, an error will be returned.
  # If multiple REST APIs are found with this name, an error will be returned. At the moment there is noi data source to
  # get REST API by ID.
  name = var.shared_services_rest_api_name
}
data "aws_api_gateway_authorizers" "unity_cognito_authorizers" {  # https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/api_gateway_authorizers
  rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
}

data "aws_api_gateway_authorizer" "unity_cognito_authorizer" {  # https://registry.terraform.io/providers/hashicorp/aws/5.30.0/docs/data-sources/api_gateway_authorizer
  rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
#  authorizer_id = data.aws_api_gateway_authorizers.unity_cognito_authorizers.ids[0]  # TODO why this is not working?
  authorizer_id = var.unity_cognito_authorizer__authorizer_id
}
##########################################################################################################################
# Creates the project API Gateway resource to be pointed to a project level API gateway.
# DEPLOYER SHOULD MODIFY THE VARIABLE var.dapa_api_prefix TO BE THE PROJECT NAME (e.g. "soundersips"). It is TIED to Lambda setting
resource "aws_api_gateway_resource" "uds_api_base_resource" {
  rest_api_id = data.aws_api_gateway_rest_api.rest_api.id
  parent_id   = data.aws_api_gateway_rest_api.rest_api.root_resource_id
  path_part   = var.dapa_api_prefix
}

#
# Creates the wildcard path (proxy+) resource, under the project resource
#
resource "aws_api_gateway_resource" "uds_all_resource" {
  rest_api_id = data.aws_api_gateway_rest_api.rest_api.id
  parent_id   = aws_api_gateway_resource.uds_api_base_resource.id
  path_part   = "{proxy+}"
}

resource "aws_api_gateway_method" "uds_all_method" {
  rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
  resource_id   = aws_api_gateway_resource.uds_all_resource.id
  http_method   = "ANY"
  authorization = "CUSTOM"
  authorizer_id = data.aws_api_gateway_authorizer.unity_cognito_authorizer.id
  request_parameters = {
    "method.request.path.proxy" = true
  }
}

resource "aws_api_gateway_method_response" "uds_all_method_response" {
    rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
    resource_id   = aws_api_gateway_resource.uds_all_resource.id
    http_method   = aws_api_gateway_method.uds_all_method.http_method
    status_code   = 200
    response_models = {
        "application/json" = "Empty"
    }
#    response_parameters = {
#        "method.response.header.Access-Control-Allow-Origin" = true
#    }
    depends_on = ["aws_api_gateway_method.uds_all_method"]
}

resource "aws_api_gateway_integration" "uds_all_lambda_integration" {
  rest_api_id   = data.aws_api_gateway_rest_api.rest_api.id
  resource_id          = aws_api_gateway_resource.uds_all_resource.id
  http_method          = aws_api_gateway_method.uds_all_method.http_method
  type                 = "AWS_PROXY"
  uri = aws_lambda_function.uds_api_1.invoke_arn
  integration_http_method = "POST"

#  cache_key_parameters = ["method.request.path.proxy"]

  timeout_milliseconds = 29000
#  request_parameters = {
#    "integration.request.path.proxy" = "method.request.path.proxy"
#  }
}


resource "aws_lambda_permission" "uds_all_lambda_integration__apigw_lambda" {
  statement_id  = "AllowExecutionFromAPIGatewayWildCard"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.uds_api_1.function_name
  principal     = "apigateway.amazonaws.com"

  # More: http://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-control-access-using-iam-policies-to-invoke-api.html
  source_arn = "arn:aws:execute-api:${var.aws_region}:${local.account_id}:${data.aws_api_gateway_rest_api.rest_api.id}/*/*/${var.dapa_api_prefix}/*"
}

##########################################################################################################################
module "cors_method" {
  source           = "./cors_module"
  rest_api_id      = data.aws_api_gateway_rest_api.rest_api.id
  resource_id      = aws_api_gateway_resource.uds_all_resource.id
}

##########################################################################################################################
# The Shared Services API Gateway deployment
resource "aws_api_gateway_deployment" "shared_services_api_gateway_deployment" {
  rest_api_id = data.aws_api_gateway_rest_api.rest_api.id
  stage_name        = var.rest_api_stage
  stage_description = "Deployed at ${timestamp()}"

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_api_gateway_integration.openapi_lambda_integration,

    aws_api_gateway_integration.stac_browser_lambda_integration,
    aws_api_gateway_integration.stac_browser_proxy_lambda_integration,
    module.cors_method.options_integration_object,
    aws_api_gateway_integration.uds_all_lambda_integration
  ]
}
