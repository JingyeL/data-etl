
resource "aws_iam_role" "lambda_role_fetcher_ftp_manager" {
  name = "${local.short_name}-lambda-fetcher-ftp-manager-role"

  assume_role_policy = data.aws_iam_policy_document.fetcher_ftp_instance_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    aws_iam_policy.lambda_policy_fetcher_ftp_manager.arn,
  ]
}

resource "aws_iam_policy" "lambda_policy_fetcher_ftp_manager" {
  name   = "${local.short_name}-lambda-policy-fetcher-ftp-manager"
  policy = data.aws_iam_policy_document.lambda_policy_fetcher_ftp_manager.json
}

data "aws_iam_policy_document" "lambda_policy_fetcher_ftp_manager" {
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
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/lambda/${local.ingestion_services_function_name}:*" 
    ]
  }


  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    resources = [
      "arn:aws:secretsmanager:${local.aws_region}:${local.account_id}:secret:poc/data/sftp/*",
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
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.config.arn}",
      "${aws_s3_bucket.config.arn}/*",
      "${aws_s3_bucket.raw_data.arn}",
      "${aws_s3_bucket.raw_data.arn}/*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "lambda:InvokeFunction"
    ]
    resources = [
      aws_lambda_function.fetcher_ftp_download.arn,
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
    ]
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
}
