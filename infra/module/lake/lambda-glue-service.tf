locals {
  glue_service_function_name = "${local.short_name}-${var.lambda_name_glue_service}"
}

resource "aws_lambda_function" "glue_service" {
  function_name    = local.glue_service_function_name
  description = "Lambda function to create and launch Glue ETL pipeline"
  package_type     = "Image"
  role             = aws_iam_role.lambda_role_glue_service.arn
  image_uri       = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${local.glue_service_function_name}:latest"
  memory_size     = 128
  ephemeral_storage {
    size = 512
  }
  timeout          = 300
  architectures = ["arm64"]
  environment {
    variables = {
      load_script_location = "s3://${aws_s3_object.job_script_load_data.bucket}/${aws_s3_object.job_script_load_data.key}"
      extra_py_files = "s3://${aws_s3_object.job_script_load_data_extra.bucket}/${aws_s3_object.job_script_load_data_extra.key}"
      glue_service_role_arn = aws_iam_role.glue_service_role.arn
      s3_data_warehouse_bucket = aws_s3_bucket.warehouse_bucket.bucket
      dynamo_table = aws_dynamodb_table.job_register.name
      config_bucket = var.config_bucket_name
      additional_python_modules = "pydantic-core==2.23.4,pydantic-settings==2.5.2,pydantic==2.9.2"
      spark_event_logs_path = "s3://${aws_s3_bucket.athena_bucket.bucket}/spark-event-logs"
      capacity_dup = 80
      chunk_size = var.data_chunk_size
      region = local.aws_region
    }
  }
}