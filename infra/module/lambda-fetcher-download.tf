locals {
  fetcher_ftp_download_function_name     = "${local.short_name}-${var.lambda_name_fetcher_ftp_download}"
}

resource "aws_lambda_function" "fetcher_ftp_download" {
  function_name    = local.fetcher_ftp_download_function_name
  description = "Lambda function to download data from sftp server"
  package_type     = "Image"
  role             = aws_iam_role.lambda_role_fetcher_ftp.arn
  image_uri       = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${local.fetcher_ftp_download_function_name}:latest"
  memory_size     = var.memory_size_service
  ephemeral_storage {
    size = 512
  }
  timeout          = 300
  architectures = ["arm64"]
  environment {
    variables = {
      RAW_DATA_BUCKET = aws_s3_bucket.raw_data.bucket
      CONFIG_BUCKET = aws_s3_bucket.config.bucket
      DYNAMO_TABLE = aws_dynamodb_table.etl_job_register.name
      region = local.aws_region
      MAX_WORKERS = var.max_fetching_workers
    }
  }
}