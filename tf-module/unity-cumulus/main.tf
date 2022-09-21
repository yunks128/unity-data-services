provider "aws" {
  region = var.aws_region

  ignore_tags {
    key_prefixes = ["gsfc-ngap"]
  }
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
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.snpp_lvl0_generate_cmr.lambda_function.lambda_handler"
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

resource "aws_lambda_function" "snpp_lvl1_generate_cmr" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-snpp_lvl1_generate_cmr"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.snpp_level1a_generate_cmr.lambda_function.lambda_handler"
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

resource "aws_lambda_function" "cumulus_granules_dapa" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-cumulus_granules_dapa"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.cumulus_granules_dapa.lambda_function.lambda_handler"
  runtime       = "python3.9"
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

resource "aws_lambda_function" "cumulus_collections_dapa" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-cumulus_collections_dapa"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.cumulus_collections_dapa.lambda_function.lambda_handler"
  runtime       = "python3.9"
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

resource "aws_lambda_function" "cumulus_collections_ingest_cnm_dapa" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-cumulus_collections_ingest_cnm_dapa"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.cumulus_granules_dapa_ingest_cnm.lambda_function.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300

  environment {
    variables = {
      LOG_LEVEL = var.log_level
      SNS_TOPIC_ARN = var.cnm_sns_topic_arn
    }
  }

  vpc_config {
    subnet_ids         = var.cumulus_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}

resource "aws_lambda_function" "cumulus_collections_creation_dapa" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-cumulus_collections_creation_dapa"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.cumulus_collections_dapa.lambda_function.lambda_handler_ingestion"
  runtime       = "python3.9"
  timeout       = 300

  environment {
    variables = {
      LOG_LEVEL = var.log_level
      CUMULUS_LAMBDA_PREFIX = var.prefix
      CUMULUS_WORKFLOW_SQS_URL = var.workflow_sqs_url
      CUMULUS_WORKFLOW_NAME = "CatalogGranule"
    }
  }

  vpc_config {
    subnet_ids         = var.cumulus_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}

resource "aws_lambda_function" "cumulus_collections_creation_dapa_facade" {
  filename      = local.lambda_file_name
  function_name = "${var.prefix}-cumulus_collections_creation_dapa_facade"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.cumulus_collections_dapa.lambda_function.lambda_handler_ingestion"
  runtime       = "python3.9"
  timeout       = 300

  environment {
    variables = {
      LOG_LEVEL = var.log_level
      CUMULUS_LAMBDA_PREFIX = var.prefix
      CUMULUS_WORKFLOW_SQS_URL = var.workflow_sqs_url
      CUMULUS_WORKFLOW_NAME = "CatalogGranule"
      COLLECTION_CREATION_LAMBDA_NAME = aws_lambda_function.cumulus_collections_creation_dapa.arn
    }
  }

  vpc_config {
    subnet_ids         = var.cumulus_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}