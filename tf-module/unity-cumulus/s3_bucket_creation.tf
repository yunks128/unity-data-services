variable "market_bucket_name" {
  type = string
  description = "name of S3 bucket. Note-1: it will be prefixed with '<project prefix>-unity-'. Note-2: It should only have '-'. '_' will be replaced with '-'"
}
variable "market_bucket__notification_prefix" {
  type = string
  default = "stage_out"
  description = "path to the directory where catalogs.json will be written"
}

data "aws_sns_topic" "uds_granules_auto_ingester_topic" {  // https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic.html
  name              = "${var.prefix}-granules_cnm_ingester"
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
    region: var.aws_region,
    s3BucketName: aws_s3_bucket.market_bucket.id,
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