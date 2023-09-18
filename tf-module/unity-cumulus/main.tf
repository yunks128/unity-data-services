provider "aws" {
  region = var.aws_region

  ignore_tags {
    key_prefixes = ["gsfc-ngap"]
  }
}
data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  lambda_file_name = "${path.module}/build/cumulus_lambda_functions_deployment.zip"
  security_group_ids_set = var.security_group_ids != null
}

resource "aws_security_group" "unity_cumulus_lambda_sg" {
  count  = local.security_group_ids_set ? 0 : 1
  vpc_id = var.cumulus_lambda_vpc_id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = var.tags
}

data "aws_iam_policy_document" "lambda_assume_role_policy" {
  statement {
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_lambda_function" "metadata_s4pa_generate_cmr" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-metadata_s4pa_generate_cmr"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.metadata_s4pa_generate_cmr.lambda_function.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300
  environment {
    variables = {
      LOG_LEVEL = var.log_level
      FILE_POSTFIX = var.metadata_s4pa_file_postfix
    }
  }

  vpc_config {
    subnet_ids         = var.cumulus_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}

resource "aws_lambda_function" "metadata_cas_generate_cmr" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-metadata_cas_generate_cmr"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.metadata_cas_generate_cmr.lambda_function.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300
  environment {
    variables = {
      LOG_LEVEL = var.log_level
    }
  }

  vpc_config {
    subnet_ids         = var.cumulus_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}

resource "aws_lambda_function" "metadata_stac_generate_cmr" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-metadata_stac_generate_cmr"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.metadata_stac_generate_cmr.lambda_function.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300
  environment {
    variables = {
      LOG_LEVEL = var.log_level
    }
  }

  vpc_config {
    subnet_ids         = var.cumulus_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}

resource "aws_lambda_function" "uds_api_1" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-uds_api_1"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.uds_api.web_service.handler"
  runtime       = "python3.9"
  timeout       = 300

  environment {
    variables = {
      CUMULUS_BASE = var.cumulus_base
      CUMULUS_LAMBDA_PREFIX = var.prefix
      LOG_LEVEL = var.log_level
      CUMULUS_WORKFLOW_SQS_URL = var.workflow_sqs_url
      CUMULUS_WORKFLOW_NAME = "CatalogGranule"
      UNITY_DEFAULT_PROVIDER = var.unity_default_provider
      COLLECTION_CREATION_LAMBDA_NAME = "arn:aws:lambda:${var.aws_region}:${local.account_id}:function:${var.prefix}-uds_api_1"
      SNS_TOPIC_ARN = var.cnm_sns_topic_arn
      UNITY_DEFAULT_PROVIDER = var.unity_default_provider
      DAPA_API_PREIFX_KEY = var.dapa_api_prefix
      ES_URL = aws_elasticsearch_domain.uds-es.endpoint
      ES_PORT = 443
      ADMIN_COMMA_SEP_GROUPS = var.comma_separated_admin_groups
    }
  }

  vpc_config {
    subnet_ids         = var.cumulus_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}

resource "aws_ssm_parameter" "uds_api_1" {
  name  = "/unity/unity-ds/api-gateway/integrations/${var.prefix}-uds_api_1-function-name"
  type  = "String"
  value = aws_lambda_function.uds_api_1.function_name
}