data "aws_sns_topic" "report_granules_topic" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic.html
  name              = var.report_granules_topic
}

resource "aws_sqs_queue" "granules_to_es_queue" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sqs_queue
  name                      = "${var.prefix}-granules_to_es_queue"
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 345600
  visibility_timeout_seconds = 310
  receive_wait_time_seconds = 0
  policy = templatefile("${path.module}/sqs_policy.json", {
    region: var.aws_region,
    roleArn: var.lambda_processing_role_arn,
    accountId: local.account_id,
    sqsName: "${var.prefix}-granules_to_es_queue",
  })
//  redrive_policy = jsonencode({
//    deadLetterTargetArn = aws_sqs_queue.terraform_queue_deadletter.arn
//    maxReceiveCount     = 4
//  })
//  tags = {
//    Environment = "production"
//  }
}

resource "aws_sns_topic_subscription" "report_granules_topic_subscription" { // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic_subscription
  topic_arn = data.aws_sns_topic.report_granules_topic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.granules_to_es_queue.arn
  filter_policy_scope = "MessageBody"  // MessageAttributes. not using attributes
  filter_policy = templatefile("${path.module}/ideas_api_job_results_filter_policy.json", {})
}

resource "aws_lambda_event_source_mapping" "granules_to_es_queue_lambda_trigger" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lambda_event_source_mapping#sqs
  event_source_arn = aws_sqs_queue.granules_to_es_queue.arn
  function_name    = aws_lambda_function.granules_to_es.arn
  batch_size = 1
  enabled = true
}