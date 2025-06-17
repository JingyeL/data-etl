locals {
  fixed_width_parser_function_name = "${local.short_name}-${var.lambda_name_fixed_width_parser}"
}
resource "aws_lambda_function" "fixed_width_parser" {
  function_name = local.fixed_width_parser_function_name
  description   = "Lambda function to transfor raw data to csv format"
  package_type  = "Image"
  role          = aws_iam_role.lambda_role_fixed_width_parser.arn
  image_uri     = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${local.fixed_width_parser_function_name}:latest"
  timeout       = 900
  memory_size   = var.memory_size_service
  ephemeral_storage {
    size = 512
  }

  architectures = ["arm64"]
  environment {
    variables = {
      ECS_CONTAINER             = local.ecs_name_data_etl_short
      ECS_CLUSTER               = aws_ecs_cluster.data_etl_ingest_cluster.name
      ECS_TASK_DEFINITION       = aws_ecs_task_definition.data_etl_def.family
      PUBLIC_SUBNET_IDS         = join(",", var.public_subnet_ids)
      ECS_SECURITY_GROUP        = aws_security_group.ecs_security_group.id
      SOURCE_DATA_BUCKET        = aws_s3_bucket.source_data.bucket
      CONFIG_BUCKET             = aws_s3_bucket.config.bucket
      RAW_DATA_BUCKET           = aws_s3_bucket.raw_data.bucket
      JOB_REGISTER_TABLE        = "glue-job-queue"
      region                    = local.aws_region
      ecs_container_memory_size = 10240
      chunk_size                = 2500

    }
  }
}
