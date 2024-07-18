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

resource "aws_lambda_function" "mock_daac_lambda" {
  filename      = local.lambda_file_name
  source_code_hash = filebase64sha256(local.lambda_file_name)
  function_name = "${var.prefix}-mock_daac_lambda"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.mock_daac.lambda_function.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300
  environment {
    variables = {
      LOG_LEVEL = var.log_level
      NO_RESPONSE_PERC = var.no_response_perc
      FAIL_PERC = var.no_response_perc
      FAIL_PERC = var.fail_perc
      UDS_ARCHIVE_SNS_TOPIC_ARN = "arn:aws:sns:${var.uds_region}:${var.uds_account}:${var.uds_prefix}-daac_archiver"
    }
  }

  vpc_config {
    subnet_ids         = var.cumulus_lambda_subnet_ids
    security_group_ids = local.security_group_ids_set ? var.security_group_ids : [aws_security_group.unity_cumulus_lambda_sg[0].id]
  }
  tags = var.tags
}

resource "aws_sns_topic" "mock_daac_cnm_sns" {
  name = "${var.prefix}-mock_daac_cnm_sns"
  tags = var.tags
}

resource "aws_sns_topic_subscription" "mock_daac_cnm_sns" { // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic_subscription
  topic_arn = aws_sns_topic.mock_daac_cnm_sns.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.mock_daac_lambda.arn
#  filter_policy_scope = "MessageBody"  // MessageAttributes. not using attributes
#  filter_policy = templatefile("${path.module}/ideas_api_job_results_filter_policy.json", {})
}