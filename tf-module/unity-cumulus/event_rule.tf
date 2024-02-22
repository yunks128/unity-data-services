# https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cloudwatch_event_rule
resource "aws_cloudwatch_event_rule" "cumulus_executions_remover_rule" {
  name = "${var.prefix}-cumulus_executions_remover_rule"
  description = "${var.prefix}-cumulus_executions_remover_rule"
  schedule_expression = "cron(0 20 * * ? *)"
}

resource "aws_cloudwatch_event_target" "cumulus_executions_remover_target" {
  target_id = "${var.prefix}-cumulus_executions_remover_target"
  rule      = aws_cloudwatch_event_rule.cumulus_executions_remover_rule.name
  arn       = aws_lambda_function.cleanup_executions.arn
}

resource "aws_lambda_permission" "cumulus_executions_remover_permission" {
  statement_id  = "${var.prefix}-cumulus_executions_remover_permission"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cleanup_executions.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cumulus_executions_remover_rule.arn
}