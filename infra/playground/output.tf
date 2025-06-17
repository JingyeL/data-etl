## S3 details
output "s3_raw_data_bucket_name" {
  value = module.this.s3_raw_data_bucket_name
}

output "s3_source_data_bucket_name" {
  value = module.this.s3_source_data_bucket_name
}

output "s3_cdm_data_bucket_name" {
  value = module.this.s3_cdm_data_bucket_name
}

output "s3_config_bucket_name" {
  value = module.this.s3_config_bucket_name
}

output "lamdba_function_schema_mapping_arn" {
  value = module.this.lamdba_function_schema_mapping_arn
}

output "lamdba_function_bulk_loader_arn" {
  value = module.this.lambda_function_bulk_loader_arn
}