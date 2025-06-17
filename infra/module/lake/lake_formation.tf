data "aws_iam_session_context" "current" {
  arn = data.aws_caller_identity.current.arn
}

resource "aws_lakeformation_resource" "this" {
  arn = aws_s3_bucket.warehouse_bucket.arn
  role_arn = aws_iam_role.lakeformation_service_role.arn
}

resource "aws_lakeformation_lf_tag" "lf_tags" {
  key    = "name"
  values = ["ndp", "new-data-platform"]
}

resource "aws_lakeformation_data_lake_settings" "this" {
  admins = [data.aws_iam_session_context.current.issuer_arn,
    aws_iam_role.data_analyst_role.arn,
  aws_iam_role.glue_service_role.arn]
  trusted_resource_owners = var.trusted_resource_owners
  create_table_default_permissions {
    principal   = "IAM_ALLOWED_PRINCIPALS"
    permissions = ["ALL", "DROP"]
  }
  create_database_default_permissions {
    principal   = "IAM_ALLOWED_PRINCIPALS"
    permissions = ["ALL", "DROP"]
  }
}

resource "aws_lakeformation_permissions" "grant_location_access_1" {
  principal   = aws_iam_role.glue_service_role.arn
  permissions = ["DATA_LOCATION_ACCESS"]
  data_location {
    arn = aws_lakeformation_resource.this.arn
  }
}

resource "aws_lakeformation_permissions" "grant_location_access_2" {
  principal   = data.aws_iam_session_context.current.issuer_arn
  permissions = ["DATA_LOCATION_ACCESS"]
  permissions_with_grant_option = ["DATA_LOCATION_ACCESS"]
  data_location {
    arn = aws_lakeformation_resource.this.arn
  }
}


resource "aws_lakeformation_permissions" "grant_db_access_1" {
  principal   = aws_iam_role.glue_service_role.arn
  permissions = ["ALL", "DROP", "CREATE_TABLE", "ALTER", "DROP", "DESCRIBE"]
  database {
    name       = aws_glue_catalog_database.this.name
    catalog_id = local.account_id
  }
}

resource "aws_lakeformation_permissions" "grant_db_access_2" {
  principal   = data.aws_iam_session_context.current.issuer_arn
  permissions = ["ALL", "DROP", "CREATE_TABLE", "ALTER", "DROP", "DESCRIBE"]
  permissions_with_grant_option = ["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE"]
  database {
    name       = aws_glue_catalog_database.this.name
    catalog_id = local.account_id
  }
}

# resource "aws_lakeformation_data_cells_filter" "demo_data_share" {
#   table_data {
#     database_name    = aws_glue_catalog_database.this.name
#     name             = "us_fl_data_share"
#     table_catalog_id = local.account_id
#     table_name       = "cfm_us_fl_company"

#     column_names = ["company_number", "name", "jurisdiction_code", "current_status", "incorporation_date", "company_type"]

#     row_filter {
#       filter_expression = "company_type='FLAL'"
#     }
#   }
# }

# resource "aws_lakeformation_permissions" "grant_table_access_1" {
#   # grant external account
#   principal   = "588738598622"
#   permissions = ["SELECT"]
#   permissions_with_grant_option = ["SELECT"]

#   data_cells_filter {
#     database_name = aws_glue_catalog_database.this.name
#     table_catalog_id = aws_glue_catalog_database.this.catalog_id
#     table_name = "cfm_us_fl_company"
#     name = "us_fl_data_share"
#   }
#   depends_on = [aws_lakeformation_data_cells_filter.demo_data_share]
# }