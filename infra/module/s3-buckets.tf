resource "aws_s3_bucket"  "raw_data" {
  bucket = "${local.short_name}-poc-data-raw" 
}

resource "aws_s3_bucket_notification" "raw_data_notification" {
  bucket      = aws_s3_bucket.raw_data.bucket
  eventbridge = true
}

resource "aws_s3_bucket"  "source_data" {
  bucket = "${local.short_name}-poc-data-source" 
}

resource "aws_s3_bucket_notification" "source_data_notification" {
  bucket      = aws_s3_bucket.source_data.id
  eventbridge = true
}

resource "aws_s3_bucket"  "cdm_data" {
  bucket = "${local.short_name}-poc-data-cdm" 
}

resource "aws_s3_bucket_notification" "cdm_data_notification" {
  bucket      = aws_s3_bucket.cdm_data.bucket
  eventbridge = true
}

resource "aws_s3_bucket"  "config" {
  bucket = "${local.short_name}-poc-data-config" 
}

resource "aws_s3_object" "config_files" {
    # for each key value pair in the map, create an object in the bucket
    for_each = var.config_files
    bucket = aws_s3_bucket.config.bucket
    key = each.key
    source = each.value
    etag = filemd5(each.value)
}