locals {
  schema_transformation_function_name = "${local.short_name}-${var.lambda_name_schema_transformation}"
}
resource "aws_lambda_function" "schema_transformation" {
  function_name = local.schema_transformation_function_name
  description   = "Lambda function to map the source data to the CDM"
  package_type  = "Image"
  role          = aws_iam_role.lambda_role_schema_transformation.arn
  image_uri     = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${local.schema_transformation_function_name}:latest"
  timeout       = 900
  memory_size   = var.memory_size_service

  architectures = ["arm64"]
  environment {
    variables = {
      ECS_CONTAINER       = local.esc_name_schema_transformation
      ECS_CLUSTER         = aws_ecs_cluster.data_etl_ingest_cluster.name
      ECS_TASK_DEFINITION = aws_ecs_task_definition.schema_transformation_def.family
      PUBLIC_SUBNET_IDS   = join(",", var.public_subnet_ids)
      ECS_SECURITY_GROUP  = aws_security_group.ecs_security_group.id
      region              = local.aws_region
      CDM_DATA_BUCKET     = aws_s3_bucket.cdm_data.bucket
      CONFIG_BUCKET       = aws_s3_bucket.config.bucket
      SOURCE_BUCKET       = aws_s3_bucket.source_data.bucket
      chunk_size          = var.data_chunk_size
    }
  }
}
