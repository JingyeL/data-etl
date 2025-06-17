locals {
  ecs_task_function_name = "${local.short_name}-${var.lambda_name_ecs_task}"
}

resource "aws_lambda_function" "ecs_task" {
  function_name    = local.ecs_task_function_name
  description      = "Lambda function to create and launch ECS task"
  package_type     = "Zip"
  filename         = module.ecs_task.archive_path
  handler          = "${var.lambda_name_ecs_task}_handler.lambda_handler"
  source_code_hash = module.ecs_task.source_code_hash
  role             = aws_iam_role.lambda_role_ecs_task.arn
  timeout          = 300
  memory_size      = var.memory_size_service
  runtime          = "python3.12"

  architectures = ["arm64"]
  environment {
    variables = {
      region          = local.aws_region
      SUBNET_IDS      = join(",", var.public_subnet_ids)
      SG_GROUPS       = aws_security_group.ecs_security_group.id
      ECS_CLUSTER     = aws_ecs_cluster.data_etl_ingest_cluster.name
      ECS_TASK_DEF    = aws_ecs_task_definition.data_etl_def.family
      ECS_CONTAINER   = local.ecs_name_data_etl_short
      INSTANCE_TYPE   = "FARGATE"
      PYTHON_RUN_TIME = "python3.12"
    }
  }
}

module "ecs_task" {
  source      = "rojopolis/lambda-python-archive/aws"
  version     = "0.1.6"
  src_dir     = "${path.module}/python/${var.lambda_name_ecs_task}/build/"
  output_path = "${path.module}/python/${var.lambda_name_ecs_task}/dist/lambda-function-${var.lambda_name_ecs_task}.zip"
}
