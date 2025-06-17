resource "aws_dynamodb_table" "job_register" {
  name         = "glue-job-queue"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "object_key"

  # "object_key": "bucket/object_key",
  # "stage": "src|stg",
  # "status": "NEW|PROCESSING|PROCESSED|ERROR"
  # "timestamp": "2021-09-01T00:00:00Z"
  attribute {
    name = "object_key"
    type = "S"
  }
  attribute {
    name = "size"
    type = "S"
  }
  attribute {
    name = "status"
    type = "S"
  }
  attribute {
    name = "timestamp"
    type = "S"
  }
  attribute {
    name = "target"
    type = "S"
  }
  global_secondary_index {
    name            = "SizeIndex"
    hash_key        = "size"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "StatusIndex"
    hash_key        = "status"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "TimestampIndex"
    hash_key       = "timestamp"
    projection_type = "ALL"
  }

    global_secondary_index {
    name            = "TargetIndex"
    hash_key       = "target"
    projection_type = "ALL"
  }


  tags = {
    Name = "job-register"
  }
}
