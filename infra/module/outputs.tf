output "s3_raw_data_bucket_name" {
  value = aws_s3_bucket.raw_data.id
}

output "s3_source_data_bucket_name" {
  value = aws_s3_bucket.source_data.id
}

output "s3_cdm_data_bucket_name" {
  value = aws_s3_bucket.cdm_data.id
}

output "s3_config_bucket_name" {
  value = aws_s3_bucket.config.id
}

output "lamdba_function_schema_mapping_arn" {
  value = aws_lambda_function.schema_transformation.arn
}

output "lambda_function_bulk_loader_arn" {
  value = aws_lambda_function.bulk_loader.arn
}

output "lambda_function_fixed_width_parser_arn" {
  value = aws_lambda_function.fixed_width_parser.arn
}

output "lambda_function_ingestion_services_arn" {
  value = aws_lambda_function.ingestion_services.arn
}

output "lambda_function_fetcher_ftp_download_arn" {
  value = aws_lambda_function.fetcher_ftp_download.arn
}

output "dynamodb_table_etl_job_register" {
  value = aws_dynamodb_table.etl_job_register.name
}

output "new_raw_data_event_rule_name" {
  value = aws_cloudwatch_event_rule.s3_new_raw_data_fixed_width_txt_created.name
}

output "new_source_data_event_rule_name" {
  value = aws_cloudwatch_event_rule.new_source_data_created.name
}

output "new_cdm_data_event_rule_name" {
  value = aws_cloudwatch_event_rule.new_cdm_data_created_for_glue.name
}

output "eventbridge_role_arn" {
  value = aws_iam_role.event_notification_role.arn
}