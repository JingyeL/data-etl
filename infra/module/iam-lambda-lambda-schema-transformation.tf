# This doc defines the IAM roles and policies required for the CDM schema mapper.
#  to interact with other AWS services.
#
# The services it interacts with are:
# - Amazon EventBridge (events:PutEvents)
# - Amazon CloudWatch Logs (logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents)
# - Amazon S3 module.source_data.arn (s3:GetObject, s3:ListBucket) 
# - Amazon S3 module.cdm_data.arn (s3:GetObject, s3:ListBucket, s3:PutObject)

resource "aws_iam_role" "lambda_role_schema_transformation" {
  name = "${local.short_name}-lambda-cdm-mapper-role"

  assume_role_policy = data.aws_iam_policy_document.schema_transformation_instance_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    aws_iam_policy.lambda_policy_schema_transformation.arn,
  ]
}

resource "aws_iam_policy" "lambda_policy_schema_transformation" {
  name   = "${local.short_name}-lambda-policy-schema_transformation"
  policy = data.aws_iam_policy_document.lambda_policy_schema_transformation.json
}

data "aws_iam_policy_document" "schema_transformation_instance_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_policy_schema_transformation" {
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
      "logs:CreateLogGroup",
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
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/lambda/${local.schema_transformation_function_name}:*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.source_data.arn}",
      "${aws_s3_bucket.cdm_data.arn}",
      "${aws_s3_bucket.config.arn}"
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.source_data.arn}/*",
      "${aws_s3_bucket.source_data.arn}",
      "${aws_s3_bucket.config.arn}",
      "${aws_s3_bucket.config.arn}/*",
      "${aws_s3_bucket.cdm_data.arn}",
      "${aws_s3_bucket.cdm_data.arn}/*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
    ]
    resources = [
      "${aws_s3_bucket.cdm_data.arn}/*",
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
      "arn:aws:ecs:${local.aws_region}:${local.account_id}:task-definition/${local.esc_name_schema_transformation}:*",
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


