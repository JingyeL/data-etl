# This doc defines the IAM roles and policies required for the Lambda bulk loader.
#  to interact with other AWS services.
#
# The services it interacts with are:
# - Amazon EventBridge (events:PutEvents)
# - Amazon CloudWatch Logs (logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents)
# - Amazon S3 (s3:GetObject, s3:ListBucket)
# - AWS Key Management Service (KMS) (kms:Decrypt, kms:Encrypt, kms:GenerateDataKey)
# - AWS Secrets Manager (secretsmanager:GetSecretValue)
# - (Todo) Amazon RDS (rds-db:connect) via proxysql

resource "aws_iam_role" "lambda_role_bulk_loader" {
  name = "${local.short_name}-lambda-bulk-loader-role"

  assume_role_policy = data.aws_iam_policy_document.bulk_loader_instance_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    aws_iam_policy.lambda_policy_bulk_loader.arn,
  ]
}

resource "aws_iam_policy" "lambda_policy_bulk_loader" {
  name   = "${local.short_name}-lambda-policy-s3-db-bulk-loader"
  policy = data.aws_iam_policy_document.lambda_policy_s3_db_bulk_loader.json
}

data "aws_iam_policy_document" "bulk_loader_instance_assume_role_policy" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_policy_s3_db_bulk_loader" {
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
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/lambda/${local.bulk_load_function_name}:*" 
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.cdm_data.arn}",
      "${aws_s3_bucket.cdm_data.arn}/*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    resources = [
      "arn:aws:secretsmanager:${local.aws_region}:${local.account_id}:secret:${var.datawarehouse_secret}-*",
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
      "rds-db:connect"
    ]
    resources = [
      "arn:aws:rds-db:${local.aws_region}:${local.account_id}:dbuser:*/${local.dbuser}"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "rds:DescribeDBProxies",
      "rds:DescribeDBProxyTargets",
      "rds:DescribeDBInstances"
    ]
    resources = ["*"]
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
}

