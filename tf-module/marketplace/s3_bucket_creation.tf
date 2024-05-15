data "aws_sns_topic" "uds_granules_auto_ingester_topic" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic.html
  name              = "${var.prefix}-granules_cnm_ingester"
}

data "aws_ssm_parameter" "uds_aws_account" {
  name = var.uds_aws_account_ssm_path
}
data "aws_ssm_parameter" "uds_prefix" {
  name = var.uds_prefix_ssm_path
}
resource "aws_s3_bucket" "market_bucket" {
  bucket = replace("${var.prefix}-unity-${var.market_bucket_name}", "_", "-")
  tags = var.tags

}

resource "aws_s3_bucket_server_side_encryption_configuration" "granules_cnm_ingester_example_bucket" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_server_side_encryption_configuration
  bucket = aws_s3_bucket.market_bucket.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "AES256"
    }
  }
}

resource "aws_s3_bucket_policy" "granules_cnm_ingester_example_bucket" {
  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_policy
  bucket = aws_s3_bucket.market_bucket.id
  policy = templatefile("${path.module}/s3_bucket_policy.json", {
    udsAwsAccount: data.aws_ssm_parameter.uds_aws_account.value,
    s3BucketName: aws_s3_bucket.market_bucket.id,
    cumulus_lambda_processing_role_name: "${data.aws_ssm_parameter.uds_prefix.value}-${var.cumulus_lambda_processing_role_name_postfix}"
  })
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.market_bucket.id
  topic {
    topic_arn     = data.aws_sns_topic.uds_granules_auto_ingester_topic.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".json"
    filter_prefix = var.market_bucket__notification_prefix
  }
}