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