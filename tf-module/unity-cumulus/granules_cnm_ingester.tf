resource "aws_lambda_function" "granules_cnm_ingester" {
  filename      = local.lambda_file_name
  source_code_hash = filebase64sha256(local.lambda_file_name)
  function_name = "${var.prefix}-granules_cnm_ingester"
  role          = var.lambda_processing_role_arn
  handler       = "cumulus_lambda_functions.granules_cnm_ingester.lambda_function.lambda_handler"
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

resource "aws_sns_topic" "granules_cnm_ingester" {
  name = "${var.prefix}-granules_cnm_ingester"
}

resource "aws_sqs_queue" "dead_letter_granules_cnm_ingester" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sqs_queue
  name                      = "${var.prefix}-dead_letter_granules_cnm_ingester"
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 345600
  visibility_timeout_seconds = 310
  receive_wait_time_seconds = 0
  policy = templatefile("${path.module}/sqs_policy.json", {
    region: var.aws_region,
    roleArn: var.lambda_processing_role_arn,
    accountId: local.account_id,
    sqsName: "${var.prefix}-dead_letter_granules_cnm_ingester",
  })
//  redrive_policy = jsonencode({
//    deadLetterTargetArn = aws_sqs_queue.terraform_queue_deadletter.arn
//    maxReceiveCount     = 4
//  })
//  tags = {
//    Environment = "production"
//  }
}

resource "aws_sqs_queue" "granules_cnm_ingester" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sqs_queue
  name                      = "${var.prefix}-granules_cnm_ingester"
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 345600
  visibility_timeout_seconds = 310
  receive_wait_time_seconds = 0
  policy = templatefile("${path.module}/sqs_policy.json", {
    region: var.aws_region,
    roleArn: var.lambda_processing_role_arn,
    accountId: local.account_id,
    sqsName: "${var.prefix}-granules_cnm_ingester",
  })
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter_granules_cnm_ingester.arn
    maxReceiveCount     = 3
  })
//  tags = {
//    Environment = "production"
//  }
}

resource "aws_sns_topic_subscription" "granules_cnm_ingester_topic_subscription" { // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic_subscription
  topic_arn = aws_sns_topic.granules_cnm_ingester.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.granules_cnm_ingester.arn
#  filter_policy_scope = "MessageBody"  // MessageAttributes. not using attributes
#  filter_policy = templatefile("${path.module}/ideas_api_job_results_filter_policy.json", {})
}

resource "aws_lambda_event_source_mapping" "granules_cnm_ingester_queue_lambda_trigger" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lambda_event_source_mapping#sqs
  event_source_arn = aws_sqs_queue.granules_cnm_ingester.arn
  function_name    = aws_lambda_function.granules_cnm_ingester.arn
  batch_size = 1
  enabled = true
}