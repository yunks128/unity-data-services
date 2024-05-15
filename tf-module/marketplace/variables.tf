variable "prefix" {
  type = string
}
variable "tags" {
  description = "Tags to be applied to Cumulus resources that support tags"
  type        = map(string)
  default     = {}
}
variable "market_bucket_name" {
  type = string
  description = "name of S3 bucket. Note-1: it will be prefixed with '<project prefix>-unity-'. Note-2: It should only have '-'. '_' will be replaced with '-'"
}
variable "market_bucket__notification_prefix" {
  type = string
  default = "stage_out"
  description = "path to the directory where catalogs.json will be written"
}
variable "uds_aws_account_ssm_path" {
  type = string
  default = "/unity/uds/account"
  description = "SSM parameter path where aws account for interacting UDS to created S3 bucket is stored"
}
variable "cumulus_lambda_processing_role_name_postfix" {
  type = string
  default = "lambda-processing"
  description = "name of the Lambda Processing role by Cumulus after `prefix`"
}

