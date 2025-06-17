resource "aws_iam_role" "data_analyst_role" {
  name               = "${local.short_name}-data-analyst-role"
  assume_role_policy = data.aws_iam_policy_document.data_analyst_role_doc.json
  managed_policy_arns = ["arn:aws:iam::aws:policy/AmazonAthenaFullAccess"]
}

resource "aws_iam_role_policy_attachment" "data_analyst_role_policy_attachment" {
  role       = aws_iam_role.data_analyst_role.name
  policy_arn = aws_iam_policy.data_analyst_role_policy.arn
}

data "aws_iam_policy_document" "data_analyst_role_doc" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [
        "arn:aws:iam::${local.account_id}:root",
      ]
    }
    effect = "Allow"
  }
}

resource "aws_iam_policy" "data_analyst_role_policy" {
  name   = "${local.short_name}-data-analyst-role-policy"
  policy = data.aws_iam_policy_document.data_analyst_role_policy.json

}

data "aws_iam_policy_document" "data_analyst_role_policy" {
  statement {
    actions = [
                "lakeformation:GetDataAccess",
                "glue:GetTable",
                "glue:GetTables",
                "glue:SearchTables",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetPartitions",
                "lakeformation:GetResourceLFTags",
                "lakeformation:ListLFTags",
                "lakeformation:GetLFTag",
                "lakeformation:SearchTablesByLFTags",
                "lakeformation:SearchDatabasesByLFTags",
                "lakeformation:StartTransaction",
                "lakeformation:CommitTransaction",
                "lakeformation:CancelTransaction",
                "lakeformation:ExtendTransaction",
                "lakeformation:DescribeTransaction",
                "lakeformation:ListTransactions",
                "lakeformation:GetTableObjects",
                "lakeformation:UpdateTableObjects",
                "lakeformation:DeleteObjectsOnCancel"
    ]
    resources = ["*"]
    effect    = "Allow"
  }

  statement {
    actions = [
      "s3:Get*",
      "s3:List*",
      "s3:Put*"
    ]
    resources = [
      "${aws_s3_bucket.warehouse_bucket.arn}",
      "${aws_s3_bucket.warehouse_bucket.arn}/*",
      "${aws_s3_bucket.athena_bucket.arn}",
      "${aws_s3_bucket.athena_bucket.arn}/*"
    ]
    effect = "Allow"
  }
}

