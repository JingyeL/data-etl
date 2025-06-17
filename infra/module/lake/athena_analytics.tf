resource "aws_s3_bucket" "athena_bucket" {
  bucket = "${local.short_name}-poc-athena"
}

resource "aws_s3_bucket_lifecycle_configuration" "athena-bucket-config" {
  bucket = aws_s3_bucket.athena_bucket.id

  rule {
    id = "exp"

      expiration {
        days = 90
      }

      filter {
        and {
          tags = {
            rule      = "exp"
            autoclean = "true"
          }
        }
      }

    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 60
      storage_class = "GLACIER"
    }
  }

}