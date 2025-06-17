resource "aws_iam_role" "lambda_role_update_meta_ts" {
  name = "${local.short_name}-lambda-role-${var.lambda_name_update_meta_ts}"

  assume_role_policy = data.aws_iam_policy_document.update_meta_ts_instance_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    aws_iam_policy.lambda_policy_update_meta_ts.arn,
  ]
}

resource "aws_iam_policy" "lambda_policy_update_meta_ts" {
  name   = "${local.short_name}-lambda-policy-update_meta_ts"
  policy = data.aws_iam_policy_document.lambda_policy_doc_update_meta_ts.json
}

data "aws_iam_policy_document" "update_meta_ts_instance_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_policy_doc_update_meta_ts" {

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
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/lambda/${local.update_meta_ts_function_name}:*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetObject",
      "s3:PutObject"
    ]
    resources = [
      "${aws_s3_bucket.raw_data.arn}",
      "${aws_s3_bucket.raw_data.arn}/*",
      "${aws_s3_bucket.source_data.arn}/*",
      "${aws_s3_bucket.source_data.arn}",
      "${aws_s3_bucket.cdm_data.arn}",
      "${aws_s3_bucket.cdm_data.arn}/*"
    ]
  }
}


