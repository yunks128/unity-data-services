variable "aws_region" {
  type    = string
  default = "us-west-2"
}
variable "tags" {
  description = "Tags to be applied to Cumulus resources that support tags"
  type        = map(string)
  default     = {}
}
variable "log_level" {
  type = string
  default = "20"
  description = "Lambda Log Level. Follow Python3 log level numbers info=20, warning=30, etc..."
}
variable "prefix" {
  type = string
}
variable "lambda_processing_role_arn" {
  type = string
}
variable "security_group_ids" {
  description = "Security Group IDs for Lambdas"
  type        = list(string)
  default     = null
}
variable "cumulus_lambda_subnet_ids" {
  description = "Subnet IDs for Lambdas"
  type        = list(string)
  default     = null
}
variable "cumulus_lambda_vpc_id" {
  type = string
}

variable "uds_archive_sns_topic_arn" {
  type = string
  default = "UDS Archive SNS Topic ARN"
}

variable "no_response_perc" {
  type = number
  default = 0.25
  description = "percentage in float form when it should not return anything. defaulted to 0.25"
}

variable "fail_perc" {
  type = number
  default = 0.25
  description = "percentage in float form when it should not return anything. defaulted to 0.25"
}

variable "uds_region" {
  type = string
  default = "us-west-2"
}
variable "uds_account" {
  type = string
}

variable "uds_prefix" {
  type = string
  default = "uds-sbx-cumulus"
  description = "UDS Prefix"
}
