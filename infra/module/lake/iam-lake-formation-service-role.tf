resource "aws_iam_role" "lakeformation_service_role" {
  name               = "AWSLakeFormationDataAdminRole-${local.short_name}"
  assume_role_policy = data.aws_iam_policy_document.lf_assume_role_policy.json
  managed_policy_arns = ["arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin",
    aws_iam_policy.lf_register_location_service_policy.arn]
}

data "aws_iam_policy_document" "lf_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lakeformation.amazonaws.com"]
    }
  }
}


resource "aws_iam_policy" "lf_register_location_service_policy" {
  name   = "${local.short_name}-lake-formation-register-location-service-policy"
  policy = data.aws_iam_policy_document.lf_register_location_service_policy.json
}

data "aws_iam_policy_document" "lf_register_location_service_policy" {
  statement {
    actions = [
      "lakeformation:RegisterResource"
    ]
    resources = ["${aws_s3_bucket.warehouse_bucket.arn}"]
    effect    = "Allow"
  }
  statement {
    actions = [
    "s3:List*",
    "s3:Batch*",
    "s3:Put*",
    "s3:Get*",
    "s3:Delete*",
    ]
    resources = [
      "${aws_s3_bucket.warehouse_bucket.arn}",
      "${aws_s3_bucket.warehouse_bucket.arn}/*"
    ]
    effect = "Allow"
  }
}