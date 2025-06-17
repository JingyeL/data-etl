locals {
  data_etl_ingest_cluster_name = "${local.short_name}-data-etl-cluster"
  ecs_name_data_etl_def        = "${local.short_name}-${var.ecs_name_data_etl}"
  ecs_name_data_etl_short      = "data_etl"
  esc_name_schema_transformation = "schema_transformation"

}

resource "aws_ecs_cluster" "data_etl_ingest_cluster" {
  name = local.data_etl_ingest_cluster_name
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name = aws_ecs_cluster.data_etl_ingest_cluster.name

  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    base              = 1
    weight            = 100
    capacity_provider = "FARGATE"
  }
}

resource "aws_ecs_task_definition" "data_etl_def" {
  family                   = local.ecs_name_data_etl_def
  execution_role_arn       = aws_iam_role.ecs_data_etl_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_data_etl_execution_role.arn
  network_mode             = "awsvpc"
  cpu                      = 2048
  memory                   = 10240
  requires_compatibilities = ["FARGATE"]
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([
    {
      name      = local.ecs_name_data_etl_short
      image     = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${local.ecs_name_data_etl_def}:latest"
      essential = true
      cpu       = 1024  
      memory    = 2048  
      environment = [
        {
          name  = "CONFIG_BUCKET"
          value = aws_s3_bucket.config.bucket
        },
        {
          name  = "RAW_DATA_BUCKET"
          value = aws_s3_bucket.raw_data.bucket
        },
        {
          name  = "SOURCE_DATA_BUCKET"
          value = aws_s3_bucket.source_data.bucket
        },
        {
          name  = "CDM_DATA_BUCKET"
          value = aws_s3_bucket.cdm_data.bucket
        },
        {
          name  = "region"
          value = local.aws_region
        },
        {
          name  = "DYNAMO_TABLE"
          value = aws_dynamodb_table.etl_job_register.name
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.ecs_data_etl_log_group.name
          "awslogs-region"        = local.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}

resource "aws_ecs_task_definition" "schema_transformation_def" {
  family                   = local.esc_name_schema_transformation
  execution_role_arn       = aws_iam_role.ecs_data_etl_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_data_etl_execution_role.arn
  network_mode             = "awsvpc"
  cpu                      = 8192
  memory                   = 16384
  requires_compatibilities = ["FARGATE"]
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }
  container_definitions = jsonencode([
    {
      name      = local.esc_name_schema_transformation
      image     = "${local.account_id}.dkr.ecr.${local.aws_region}.amazonaws.com/${local.ecs_name_data_etl_def}:latest"
      essential = true
      cpu       = 8192  
      memory    = 16384  
      environment = [
        {
          name  = "CONFIG_BUCKET"
          value = aws_s3_bucket.config.bucket
        },
        {
          name  = "RAW_DATA_BUCKET"
          value = aws_s3_bucket.raw_data.bucket
        },
        {
          name  = "SOURCE_DATA_BUCKET"
          value = aws_s3_bucket.source_data.bucket
        },
        {
          name  = "CDM_DATA_BUCKET"
          value = aws_s3_bucket.cdm_data.bucket
        },
        {
          name  = "region"
          value = local.aws_region
        },
        {
          name  = "DYNAMO_TABLE"
          value = aws_dynamodb_table.etl_job_register.name
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.ecs_schema_transformation_log_group.name
          "awslogs-region"        = local.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])
}



resource "aws_cloudwatch_log_group" "ecs_data_etl_log_group" {
  name              = "/aws/ecs/${local.ecs_name_data_etl_def}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "ecs_schema_transformation_log_group" {
  name              = "/aws/ecs/${local.esc_name_schema_transformation}"
  retention_in_days = 14
}
