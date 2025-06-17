locals {
  load_data_script = "glue_job_scripts/data_load.py"
  load_data_script_extra = "glue_job_scripts/glue_job.zip"
}

resource "aws_s3_bucket" "warehouse_bucket" {
  bucket = "${local.short_name}-poc-data-warehouse" 
}

resource "aws_s3_bucket_notification" "warehouse_bucket_notification" {
  bucket      = aws_s3_bucket.warehouse_bucket.bucket
  eventbridge = true
}

resource "aws_s3_object" "job_script_load_data" {
  bucket = var.config_bucket_name
  key    = local.load_data_script
  source = "${path.module}/dist/${local.load_data_script}"
  etag = filemd5("${path.module}/dist/${local.load_data_script}")
}

resource "aws_s3_object" "job_script_load_data_extra" {
  bucket = var.config_bucket_name
  key    = local.load_data_script_extra
  source = "${path.module}/dist/${local.load_data_script_extra}"
  etag = filemd5("${path.module}/dist/${local.load_data_script_extra}")
}


resource "aws_ecr_repository" "glue_service" {
  name = local.glue_service_function_name
}


resource "aws_glue_catalog_database" "this" {
  name        = var.catalog_db_name
  description = "the data located in ${aws_s3_bucket.warehouse_bucket.bucket}"
  location_uri = "${aws_s3_bucket.warehouse_bucket.arn}"
  create_table_default_permission {
      principal {
        data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
    permissions = ["ALL"]
  }
}
