resource "aws_iam_role" "glue_service_role" {
  name = "AWSGlueServiceRole-${local.short_name}"
  managed_policy_arns = ["arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
  "${aws_iam_policy.glue_service_role_policy.arn}",
  "${aws_iam_policy.data_analyst_role_policy.arn}"]
  assume_role_policy = data.aws_iam_policy_document.glue_service_role_doc.json
}


data "aws_iam_policy_document" "glue_service_role_doc" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type = "Service"
      identifiers = [
        "glue.amazonaws.com",
        "lakeformation.amazonaws.com"
      ]
    }
  }
}

resource "aws_iam_policy" "glue_service_role_policy" {
  name   = "${local.short_name}-glue-service-role-policy"
  policy = data.aws_iam_policy_document.glue_service_role_policy.json
}

data "aws_iam_policy_document" "glue_service_role_policy" {
  statement {
    actions = [
      "lakeformation:GrantPermissions",
      "lakeformation:GetDataAccess"
    ]
    resources = ["*"]
    effect    = "Allow"
  }

  statement {
    actions = ["iam:PassRole"]
    resources = [
      "arn:aws:iam::${local.account_id}:role/AWSGlueServiceRole-${local.short_name}"
    ]
    effect = "Allow"
  }

  statement {
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
      "arn:aws:s3:::${var.config_bucket_name}/*"
    ]
    effect = "Allow"
  }
  statement {
    actions = [
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.warehouse_bucket.arn}",
      "${aws_s3_bucket.warehouse_bucket.arn}/*"
    ]
    effect = "Allow"
  }

  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws-glue/*",
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/:*"
    ]
    effect    = "Allow"
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
