provider "aws" {
  region = var.aws_region

  ignore_tags {
    key_prefixes = ["gsfc-ngap"]
  }
}

data "aws_iam_role" "unity_cumulus_lambda_role" {
  name                 = "${var.prefix}-PublishExecutionsLambda"
}

locals {
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

resource "aws_lambda_function" "snpp_lvl0_generate_cmr" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-snpp_lvl0_generate_cmr"
  role          = data.aws_iam_role.unity_cumulus_lambda_role.arn
  handler       = "cumulus_lambda_functions.snpp_lvl0_generate_cmr.lambda_function.lambda_handler"
  runtime       = "python3.7"
  timeout       = 300


  vpc_config {
    subnet_ids         = var.cumulus_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}

resource "aws_lambda_function" "cumulus_granules_dapa" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-cumulus_granules_dapa"
  role          = data.aws_iam_role.unity_cumulus_lambda_role.arn
  handler       = "cumulus_lambda_functions.cumulus_granules_dapa.lambda_function.lambda_handler"
  runtime       = "python3.7"
  timeout       = 300

  environment {
    variables = {
      CUMULUS_BASE = var.cumulus_base
      CUMULUS_LAMBDA_PREFIX = var.prefix
    }
  }

  vpc_config {
    subnet_ids         = var.cumulus_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}