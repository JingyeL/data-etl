resource "aws_dynamodb_table" "etl_job_register" {
  name         = "etl-job-queue"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "jurisdiction" 
  range_key    = "object_key"

  attribute {
    name = "jurisdiction"
    type = "S"
  }

  attribute {
    name = "object_key"
    type = "S"
  }

  attribute {
    name = "periodicity"
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
  
  attribute {
    name = "ingest_config"
    type = "S"
  }

  local_secondary_index {
    name            = "PeriodicityIndex"
    range_key       = "periodicity"
    projection_type = "ALL"
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

  local_secondary_index {
    name            = "ConfIndex"
    range_key       = "ingest_config"
    projection_type = "ALL"
  }

  tags = {
    Name = "etl-job-queue"
  }
}
