output "metadata_s4pa_generate_cmr_arn" {
  value = aws_lambda_function.metadata_s4pa_generate_cmr.arn
}

output "metadata_cas_generate_cmr_arn" {
    value = aws_lambda_function.metadata_cas_generate_cmr.arn
}

output "metadata_stac_generate_cmr_arn" {
    value = aws_lambda_function.metadata_stac_generate_cmr.arn
}
