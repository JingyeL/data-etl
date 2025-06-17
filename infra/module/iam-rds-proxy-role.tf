# define iam role for rds proxy
resource "aws_iam_role" "rds_proxy_role" {
    name = "${local.short_name}-rds-proxy-role"
    assume_role_policy = data.aws_iam_policy_document.rds_proxy_assume_role.json
}

data "aws_iam_policy_document" "rds_proxy_assume_role" {
    statement {
        actions = ["sts:AssumeRole"]
        principals {
        type        = "Service"
        identifiers = ["rds.amazonaws.com"]
        }
    }
}

resource "aws_iam_role_policy_attachment" "rds_proxy_role_policy_attachment" {
    role       = aws_iam_role.rds_proxy_role.name
    policy_arn = aws_iam_policy.rds_proxy_role_policy.arn
}

resource "aws_iam_policy" "rds_proxy_role_policy" {
    name        = "${local.short_name}-rds-proxy-role-policy"
    description = "Policy for RDS Proxy Role"
    policy      = data.aws_iam_policy_document.rds_proxy_role_policy_doc.json
  
}

data "aws_iam_policy_document" "rds_proxy_role_policy_doc" {
    statement {
        effect = "Allow"
        actions = [
            "rds-db:connect"
        ]
        resources = [
            aws_db_instance.datawarehouse.arn
        ]
    }
    statement {
        effect = "Allow"
        actions = [
            "secretsmanager:GetSecretValue"
        ]
        resources = [
            aws_secretsmanager_secret.datawarehouse_secret.arn
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
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
        ]
        resources = [
            "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/rds-proxy/*"
        ]
    }
}