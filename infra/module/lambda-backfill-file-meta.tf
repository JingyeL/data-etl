locals {
  backfill_file_meta_function_name = "${local.short_name}-${var.lambda_name_backfill_file_meta}"
}
resource "aws_lambda_function" "backfill_file_meta" {
  function_name    = local.backfill_file_meta_function_name
  description      = "Lambda function to backfill file metadata"
  package_type     = "Zip"
  filename         = module.backfill_file_meta_archive.archive_path
  handler          = "${var.lambda_name_backfill_file_meta}_handler.lambda_handler"
  source_code_hash = module.backfill_file_meta_archive.source_code_hash
  role             = aws_iam_role.lambda_role_backfill_file_meta.arn
  timeout          = 300
  memory_size      = var.memory_size_service
  runtime          = "python3.12"

  architectures = ["arm64"]
    environment {
    variables = {
      region               = local.aws_region
    }
  }
}

module "backfill_file_meta_archive" {
  source               = "rojopolis/lambda-python-archive/aws"
  version              = "0.1.6"
  src_dir              = "${path.module}/python/${var.lambda_name_backfill_file_meta}/build/"
  output_path          = "${path.module}/python/${var.lambda_name_backfill_file_meta}/dist/lambda-function-${var.lambda_name_backfill_file_meta}.zip"
}