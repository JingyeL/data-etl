resource "aws_dynamodb_table" "data_bulk_load_job_register" {
  name         = "data-bulk-load-job-queue"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "object_key" 

  attribute {
    name = "object_key"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "N"
  }

  attribute {
    name = "status"
    type = "S"
  }

  local_secondary_index {
    name            = "StatusIndex"
    range_key       = "status"
    projection_type = "ALL"
  }

  local_secondary_index {
    name            = "TimestampIndex"
    range_key       = "timestamp"
    projection_type = "ALL"
  }


  tags = {
    Name = "data-bulk-load-queue"
  }
}
