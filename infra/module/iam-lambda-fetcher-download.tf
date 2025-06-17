resource "aws_iam_role" "lambda_role_fetcher_ftp" {
  name = "${local.short_name}-lambda-fetcher-ftp-role"

  assume_role_policy = data.aws_iam_policy_document.fetcher_ftp_instance_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    aws_iam_policy.lambda_policy_fetcher_ftp.arn,
  ]
}



resource "aws_iam_policy" "lambda_policy_fetcher_ftp" {
  name   = "${local.short_name}-lambda-policy-fetcher-ftp"
  policy = data.aws_iam_policy_document.lambda_policy_fetcher_ftp.json
}

data "aws_iam_policy_document" "fetcher_ftp_instance_assume_role_policy" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_policy_fetcher_ftp" {
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
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/lambda/${local.fetcher_ftp_download_function_name}:*" 
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
      "${aws_s3_bucket.raw_data.arn}/*"
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
      "ecr:GetDownloadUrlForLayer",
      "ecr:BatchGetImage",
      "ecr:GetAuthorizationToken",
      "ecr:BatchCheckLayerAvailability"
    ]
    resources = ["arn:aws:ecr:${local.aws_region}:${local.account_id}:repository/*"]
  }
}
