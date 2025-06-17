locals {
  archive_utility_function_name = "${local.short_name}-${var.lambda_name_archive_utilty}"
}

resource "aws_lambda_function" "archive_utility" {
  function_name = local.archive_utility_function_name
  description   = "Lambda function to creating and extracting zip archive"
  package_type  = "Image"
  role          = aws_iam_role.lambda_role_archive_utility.arn
  image_uri     = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${local.archive_utility_function_name}:latest"
  memory_size   = var.memory_size_etl
  ephemeral_storage {
    size = 512
  }
  architectures = ["arm64"]
    environment {
    variables = {
      region               = local.aws_region
    }
  }
  timeout = 900
}
