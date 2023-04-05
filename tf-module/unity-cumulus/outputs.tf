output "metadata_s4pa_generate_cmr_arn" {
  value = aws_lambda_function.metadata_s4pa_generate_cmr.arn
}

output "cumulus_granules_dapa_arn" {
  value = aws_lambda_function.cumulus_granules_dapa.arn
}

output "cumulus_collections_dapa_arn" {
  value = aws_lambda_function.cumulus_collections_dapa.arn
}

output "metadata_cas_generate_cmr_arn" {
    value = aws_lambda_function.metadata_cas_generate_cmr.arn
}

output "cumulus_collections_ingest_cnm_dapa_arn" {
    value = aws_lambda_function.cumulus_collections_ingest_cnm_dapa.arn
}