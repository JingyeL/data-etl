resource "aws_iam_role" "lambda_role_archive_utility" {
  name = "${local.short_name}-lambda-archive_utility-role"

  assume_role_policy = data.aws_iam_policy_document.archive_utility_instance_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    aws_iam_policy.lambda_archive_utility.arn,
  ]
}


resource "aws_iam_policy" "lambda_archive_utility" {
  name   = "${local.short_name}-lambda-policy-rchive-utility"
  policy = data.aws_iam_policy_document.lambda_policy_archive_utility.json
}

data "aws_iam_policy_document" "archive_utility_instance_assume_role_policy" {
  statement {
    effect = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_policy_archive_utility" {
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
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/lambda/${local.archive_utility_function_name}:*" 
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
