output "snpp_lvl0_generate_cmr_arn" {
  value = aws_lambda_function.snpp_lvl0_generate_cmr.arn
}

output "cumulus_granules_dapa_arn" {
  value = aws_lambda_function.cumulus_granules_dapa.arn
}

output "cumulus_collections_dapa_arn" {
  value = aws_lambda_function.cumulus_collections_dapa.arn
}