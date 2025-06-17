locals {
  bulk_load_function_name     = "${local.short_name}-${var.lambda_name_bulk_loader}"
}

resource "aws_lambda_function" "bulk_loader" {
  function_name    = local.bulk_load_function_name
  description = "Lambda function to bulk load data from cdm bucket into the datawarehouse"
  package_type     = "Image"
  role             = aws_iam_role.lambda_role_bulk_loader.arn
  image_uri       = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${local.bulk_load_function_name}:latest"
  memory_size     = var.memory_size_service
  timeout          = 900
  architectures = ["arm64"]
  environment {
    variables = {
      region       = local.aws_region
      DB_CONNECTION = var.datawarehouse_secret 
    }
  }
  vpc_config {
    subnet_ids         = var.rds_subnet_ids
    security_group_ids = [aws_security_group.lambda_sg.id] 
  }
}