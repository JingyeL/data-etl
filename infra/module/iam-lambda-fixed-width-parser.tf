resource "aws_iam_role" "lambda_role_fixed_width_parser" {
  name = "${local.short_name}-lambda-fixed-width-parser-role"

  assume_role_policy = data.aws_iam_policy_document.fixed_width_parser_instance_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    aws_iam_policy.lambda_policy_fixed_width_parser.arn,
  ]
}

resource "aws_iam_policy" "lambda_policy_fixed_width_parser" {
  name   = "${local.short_name}-lambda-policy-fixed-width-parser"
  policy = data.aws_iam_policy_document.lambda_policy_fixed_width_parser.json
}

data "aws_iam_policy_document" "fixed_width_parser_instance_assume_role_policy" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_policy_fixed_width_parser" {
  statement {
    effect = "Allow"
    actions = [
      "events:PutEvents"
    ]
    resources = [
      local.default_event_bus_arn
    ]
  }

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
      "logs:PutLogEvents",
      "cloudwatch:PutMetricData"
    ]
    resources = [
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/lambda/${local.fixed_width_parser_function_name}:*" 
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.raw_data.arn}/*",
      "${aws_s3_bucket.config.arn}/*",
      "${aws_s3_bucket.source_data.arn}/*"
    ]
  }


  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.raw_data.arn}",
      "${aws_s3_bucket.config.arn}",
    ]
  }

    statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
    ]
    resources = [
      "${aws_s3_bucket.source_data.arn}/*",
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
      "iam:PassRole"
    ]
    resources = [
      aws_iam_role.ecs_data_etl_execution_role.arn
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
        "arn:aws:dynamodb:${local.aws_region}:${local.account_id}:table/glue-job-queue",
    ]
  }
}

