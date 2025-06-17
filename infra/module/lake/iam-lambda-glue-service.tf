resource "aws_iam_role" "lambda_role_glue_service" {
  name = "${local.short_name}-lambda-role-${var.lambda_name_glue_service}"

  assume_role_policy = data.aws_iam_policy_document.glue_service_instance_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "${aws_iam_policy.lambda_policy_glue_service.arn}"
  ]
}

data "aws_iam_policy_document" "glue_service_instance_assume_role_policy" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "lambda_policy_glue_service" {
  name   = "${local.short_name}-lambda-policy-${var.lambda_name_glue_service}"
  policy = data.aws_iam_policy_document.lambda_policy_glue_service.json
}

data "aws_iam_policy_document" "lambda_policy_glue_service" {
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
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/lambda/${local.glue_service_function_name}:*" 
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      "arn:aws:s3:::${var.raw_data_bucket_name}",
      "arn:aws:s3:::${var.raw_data_bucket_name}/*",
      "arn:aws:s3:::${var.source_data_bucket_name}",
      "arn:aws:s3:::${var.source_data_bucket_name}/*",
      "arn:aws:s3:::${var.cdm_data_bucket_name}",
      "arn:aws:s3:::${var.cdm_data_bucket_name}/*",
      "arn:aws:s3:::${var.config_bucket_name}",
      "arn:aws:s3:::${var.config_bucket_name}/*",
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
      "glue:GetWorkflow",
      "glue:StartWorkflowRun",
      "glue:CreateWorkflow",
      "glue:CreateTrigger",
      "glue:CreateJob",
      "glue:CreateCrawler",
      "glue:StartTrigger",
      "glue:StartJobRun",
      "glue:StartCrawler"
    ]
      resources = ["arn:aws:glue:${local.aws_region}:${local.account_id}:*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "iam:PassRole"
    ]
    resources = ["arn:aws:iam::${local.account_id}:role/AWSGlueServiceRole-${local.short_name}"]
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
        "${aws_dynamodb_table.job_register.arn}",
    ]
  }
}

