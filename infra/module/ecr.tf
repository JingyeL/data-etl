resource "aws_ecr_repository" "cdm_mapper" {
  name = "${local.short_name}-${var.lambda_name_schema_transformation}"
}

resource "aws_ecr_repository" "bulk_loader" {
  name = "${local.short_name}-${var.lambda_name_bulk_loader}"
}

resource "aws_ecr_repository" "fixed_width_parser" {
  name = "${local.short_name}-${var.lambda_name_fixed_width_parser}"
}

resource "aws_ecr_repository" "ingestion_services" {
  name = "${local.short_name}-${var.lambda_name_ingestion_services}"
}

resource "aws_ecr_repository" "fetcher_ftp_download" {
  name = "${local.short_name}-${var.lambda_name_fetcher_ftp_download}"
}

resource "aws_ecr_repository" "esc_data_etl" {
  name = "${local.short_name}-${var.ecs_name_data_etl}"
}

resource "aws_ecr_repository" "backfill_file_meta" {
  name = "${local.short_name}-${var.lambda_name_backfill_file_meta}"
}

resource "aws_ecr_repository" "archive_utility" {
  name = "${local.short_name}-${var.lambda_name_archive_utilty}"
}
