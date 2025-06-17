# give me the template for iam-glue-job-source-role
# including role, assum role policy, policy and document and policy attachment
# in terraform

resource "aws_iam_role" "glue_job_role" {
  name = "${local.short_name}-glue-job-role"
  assume_role_policy = data.aws_iam_policy_document.glue_job_instance_assume_role_policy.json 
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]
}

resource "aws_iam_policy_attachment" "glue_job_policy_attachment" {
  name       = "${local.short_name}-glue-job-policy-attachment"
  roles      = [aws_iam_role.glue_job_role.name]
  policy_arn = aws_iam_policy.glue_job_policy.arn
}

data "aws_iam_policy_document" "glue_job_instance_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = [
        "glue.amazonaws.com"
      ]
    }
  }
}

resource "aws_iam_policy" "glue_job_policy" {
  name   = "${local.short_name}-glue-job-source-policy"
  policy = data.aws_iam_policy_document.glue_job_policy.json
}

data "aws_iam_policy_document" "glue_job_policy" {
  
    statement {
    actions = [
				"s3:GetObject",
				"s3:ListObject"
    ]
    resources = [
      "arn:aws:s3:::${var.source_data_bucket_name}",
      "arn:aws:s3:::${var.source_data_bucket_name}/*",
      "arn:aws:s3:::${var.cdm_data_bucket_name}",
      "arn:aws:s3:::${var.cdm_data_bucket_name}/*"
    ]
    effect = "Allow"
    }
  
  statement {
    actions = [
				"s3:Abort*",
				"s3:DeleteObject*",
				"s3:GetBucket*",
				"s3:GetObject*",
				"s3:List*",
				"s3:PutObject",
				"s3:PutObjectLegalHold",
				"s3:PutObjectRetention",
				"s3:PutObjectTagging",
				"s3:PutObjectVersionTagging"
    ]
    resources = [
      "${aws_s3_bucket.warehouse_bucket.arn}",
      "${aws_s3_bucket.warehouse_bucket.arn}/*"
    ]
    effect = "Allow"
  }

  statement {
    actions = [
				"s3:GetObject",
				"s3:ListBucket",
    ]
    resources = [
      "arn:aws:s3:::${var.config_bucket_name}/*",
      "arn:aws:s3:::${var.config_bucket_name}"

    ]
    effect = "Allow"
  }

  statement	{
			actions = [
				"logs:CreateLogGroup",
				"logs:CreateLogStream",
				"logs:PutLogEvents"
			]
			resources = [
                "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws-glue/*"
                ]
			effect = "Allow"
		}
}



