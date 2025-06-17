locals {
  update_meta_ts_function_name = "${local.short_name}-${var.lambda_name_update_meta_ts}"
}
resource "aws_lambda_function" "update_meta_ts" {
  function_name    = local.update_meta_ts_function_name
  description      = "Lambda function to backfill file metadata"
  package_type     = "Zip"
  filename         = module.update_meta_ts_archive.archive_path
  handler          = "${var.lambda_name_update_meta_ts}_handler.lambda_handler"
  source_code_hash = module.update_meta_ts_archive.source_code_hash
  role             = aws_iam_role.lambda_role_update_meta_ts.arn
  timeout          = 900
  memory_size      = var.memory_size_service
  runtime          = "python3.12"

  architectures = ["arm64"]
    environment {
    variables = {
      region = local.aws_region
    }
  }
}

module "update_meta_ts_archive" {
  source               = "rojopolis/lambda-python-archive/aws"
  version              = "0.1.6"
  src_dir              = "${path.module}/python/${var.lambda_name_update_meta_ts}/build/"
  output_path          = "${path.module}/python/${var.lambda_name_update_meta_ts}/dist/lambda-function-${var.lambda_name_update_meta_ts}.zip"
}