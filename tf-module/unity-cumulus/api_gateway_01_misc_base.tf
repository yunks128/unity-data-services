resource "aws_api_gateway_resource" "misc_base_resource" {
  rest_api_id = data.aws_api_gateway_rest_api.rest_api.id
  parent_id   = aws_api_gateway_resource.uds_api_base_resource.id
  path_part   = "misc"
}

module "misc_base_any_to_lambda_module" {
  source = "./any_to_lambda_module"
  authorizer_id = data.aws_api_gateway_authorizer.unity_cognito_authorizer.id
  lambda_invoke_arn = aws_lambda_function.uds_api_1.invoke_arn
  rest_api_id      = data.aws_api_gateway_rest_api.rest_api.id
  resource_id      = aws_api_gateway_resource.misc_base_resource.id
}


module "misc_base_cors_method" {
  source           = "./cors_module"
  rest_api_id      = data.aws_api_gateway_rest_api.rest_api.id
  resource_id      = aws_api_gateway_resource.misc_base_resource.id
  prefix           = "${var.prefix}_misc_base"
}