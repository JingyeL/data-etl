resource "aws_iam_role" "s3_import_role" {
  name = "${local.short_name}-s3-import-role"
  description = "Role for Import from S3 to RDS"
  assume_role_policy = data.aws_iam_policy_document.s3_import_assume_role.json
}

data "aws_iam_policy_document" "s3_import_assume_role" {
    statement {
        actions = ["sts:AssumeRole"]
        principals {
        type        = "Service"
        identifiers = ["rds.amazonaws.com"]
        }
    }
}

resource "aws_iam_role_policy_attachment" "s3_import_role_policy_attachment" {
    role       = aws_iam_role.s3_import_role.name
    policy_arn = aws_iam_policy.s3_import_role_policy.arn
}

resource "aws_iam_policy" "s3_import_role_policy" {
    name        = "${local.short_name}-s3-import-role-policy"
    description = "Policy for S3 Import Role"
    policy      = data.aws_iam_policy_document.s3_import_role_policy_doc.json
}

data "aws_iam_policy_document" "s3_import_role_policy_doc" {
    statement {
        effect = "Allow"
        actions = [
            "s3:GetObject",
            "s3:GetObjectVersion",
            "s3:ListBucket"
        ]
        resources = [
            "${aws_s3_bucket.source_data.arn}",
            "${aws_s3_bucket.source_data.arn}/*",
            "${aws_s3_bucket.cdm_data.arn}",
            "${aws_s3_bucket.cdm_data.arn}/*", 
            "${aws_s3_bucket.config.arn}",
            "${aws_s3_bucket.config.arn}/*"
        ]
    }
}