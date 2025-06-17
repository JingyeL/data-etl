locals {
  ingestion_services_function_name = "${local.short_name}-${var.lambda_name_ingestion_services}"
}

resource "aws_lambda_function" "ingestion_services" {
  function_name = local.ingestion_services_function_name
  description   = "Lambda function to manage data download from sftp server"
  package_type  = "Image"
  role          = aws_iam_role.lambda_role_fetcher_ftp_manager.arn
  image_uri     = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${local.ingestion_services_function_name}:latest"
  memory_size   = var.memory_size_service
  ephemeral_storage {
    size = 512
  }
  timeout       = 300
  architectures = ["arm64"]
  environment {
    variables = {
      CONFIG_BUCKET            = aws_s3_bucket.config.bucket
      RAW_DATA_BUCKET          = aws_s3_bucket.raw_data.bucket
      LAMBDA_FUNC_FTP_DOWNLOAD = aws_lambda_function.fetcher_ftp_download.function_name
      DYNAMO_TABLE             = aws_dynamodb_table.etl_job_register.name
      ECS_CONTAINER            = local.ecs_name_data_etl_short
      ECS_CLUSTER              = aws_ecs_cluster.data_etl_ingest_cluster.name
      ECS_TASK_DEFINITION      = aws_ecs_task_definition.data_etl_def.family
      PUBLIC_SUBNET_IDS        = join(",", var.public_subnet_ids)
      ECS_SECURITY_GROUP       = aws_security_group.ecs_security_group.id
      region                   = local.aws_region
    }
  }
}

resource "aws_lambda_permission" "allow_fetcher_ftp_invoke_downloader" {
  statement_id  = "AllowExecutionFromfetcherFtp"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fetcher_ftp_download.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_lambda_function.ingestion_services.arn
}
