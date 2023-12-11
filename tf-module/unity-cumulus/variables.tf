variable "log_level" {
  type = string
  default = "20"
  description = "Lambda Log Level. Follow Python3 log level numbers info=20, warning=30, etc..."
}
variable "account_id" {
  type = string
  description = "AWS Account ID"
}
variable "metadata_stac_file_postfix" {
  type = string
  description = "Comma separated File Postfix for STAC JSON metadata files"
  default = "STAC.JSON"
}
variable "metadata_s4pa_file_postfix" {
  type = string
  description = "Comma separated File Postfix for PDS XML metadata files"
}
variable "prefix" {
  type = string
}
variable "aws_region" {
  type    = string
  default = "us-west-2"
}
variable "cumulus_lambda_subnet_ids" {
  description = "Subnet IDs for Lambdas"
  type        = list(string)
  default     = null
}
variable "cumulus_lambda_vpc_id" {
  type = string
}
variable "permissions_boundary_arn" {
  type    = string
  default = null
}
variable "security_group_ids" {
  description = "Security Group IDs for Lambdas"
  type        = list(string)
  default     = null
}

variable "tags" {
  description = "Tags to be applied to Cumulus resources that support tags"
  type        = map(string)
  default     = {}
}

variable "cnm_sns_topic_arn" {
  description = "SNS ARN of CNM submission topic"
  type = string
}

variable "workflow_sqs_url" {
  type = string
  description = "SNS ARN of CNM submission topic"
}

variable "unity_default_provider" {
  type = string
  description = "default provider name"

}

variable "dapa_api_prefix" {
  type = string
  default = "am-uds-dapa"
}

variable "uds_base_url" {
  type = string
}

variable "report_to_ems" {
  type = string
  default = "TRUE"
}

variable "cumulus_base" {
  type = string
  description = "Cumulus base URL. Example: https://axhmoecy02.execute-api.us-west-2.amazonaws.com/dev"
}

variable "register_custom_metadata" {
  type = string
  default = "TRUE"
  description = "flag to decide if custom metadata will be added. "
}

variable "lambda_processing_role_arn" {
  type = string
}

variable "uds_es_cluster_instance_count" {
  type = number
  default = 2
  description = "How many EC2 instances for Opensearch"
}

variable "uds_es_cluster_instance_type" {
  type = string
  default = "r5.large.elasticsearch"
  description = "EC2 instance type for Opensearch"
}

variable "comma_separated_admin_groups" {
  type = string
  description = "comma separated cognito groups which will be authorized as ADMIN group"
}