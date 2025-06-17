resource "aws_iam_role" "lambda_role_backfill_file_meta" {
  name = "${local.short_name}-lambda-backfill-file-meta-role"

  assume_role_policy = data.aws_iam_policy_document.backfill_file_meta_instance_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    aws_iam_policy.lambda_policy_backfill_file_meta.arn,
  ]
}

resource "aws_iam_policy" "lambda_policy_backfill_file_meta" {
  name   = "${local.short_name}-lambda-policy-backfill_file_meta"
  policy = data.aws_iam_policy_document.lambda_policy_doc_backfill_file_meta.json
}

data "aws_iam_policy_document" "backfill_file_meta_instance_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_policy_doc_backfill_file_meta" {

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
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/lambda/${local.backfill_file_meta_function_name}:*"
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
      "arn:aws:s3:::oc-poc-data-raw/*",
      "arn:aws:s3:::oc-poc-data-raw"
    ]
  }
}


