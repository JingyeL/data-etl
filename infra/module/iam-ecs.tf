resource "aws_iam_role" "ecs_data_etl_execution_role" {
  name = "${local.short_name}-ecs-data_etl-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
  managed_policy_arns = ["arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
  aws_iam_policy.ecs_data_etl_execution_role_policy.arn]
}

resource "aws_iam_policy" "ecs_data_etl_execution_role_policy" {
  name        = "${local.short_name}-ecs-data_etl-execution-role-policy"
  description = "Policy for Data ETL Execution Role"
  policy      = data.aws_iam_policy_document.ecs_data_etl_execution_role_policy_doc.json
}

data "aws_iam_policy_document" "ecs_data_etl_execution_role_policy_doc" {

  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup"
    ]
    resources = [
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/ecs/${local.ecs_name_data_etl_def}:*",

    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject"
    ]
    resources = [
      "${aws_s3_bucket.raw_data.arn}",
      "${aws_s3_bucket.raw_data.arn}/*",
      "${aws_s3_bucket.source_data.arn}",
      "${aws_s3_bucket.source_data.arn}/*",
      "${aws_s3_bucket.cdm_data.arn}",
      "${aws_s3_bucket.cdm_data.arn}/*",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.config.arn}",
      "${aws_s3_bucket.config.arn}/*",

    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "dynamodb:BatchGetItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:GetItem"
    ]
    resources = [
      "${aws_dynamodb_table.etl_job_register.arn}",
      "arn:aws:dynamodb:${local.aws_region}:${local.account_id}:table/glue-job-queue",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    resources = [
      "arn:aws:secretsmanager:${local.aws_region}:${local.account_id}:secret:poc/data/sftp/*",
      "arn:aws:secretsmanager:${local.aws_region}:${local.account_id}:secret:poc/data/open-corporate-data-poc/*",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "kms:Decrypt"
    ]
    resources = [
      aws_kms_key.sops.arn
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "ecr:GetAuthorizationToken",
      "ecr:BatchCheckLayerAvailability"
    ]
    resources = ["arn:aws:ecr:${local.aws_region}:${local.account_id}:repository/*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "ecs:DescribeClusters",
      "ecs:DescribeTaskDefinition",
      "ecs:ListTasks",
      "ecs:RunTask"
    ]
    resources = [
      "arn:aws:ecs:${local.aws_region}:${local.account_id}:task-definition/${local.ecs_name_data_etl_def}:*",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface",
      "ec2:AttachNetworkInterface",
      "ec2:DetachNetworkInterface"
    ]
    resources = concat(
      [
        "arn:aws:ec2:${local.aws_region}:${local.account_id}:network-interface/*",
        "arn:aws:ec2:${local.aws_region}:${local.account_id}:security-group/${aws_security_group.ecs_security_group.id}"
      ],
      [
        for subnet_id in var.public_subnet_ids : "arn:aws:ec2:${local.aws_region}:${local.account_id}:subnet/${subnet_id}"
      ]
    )
  }

}
