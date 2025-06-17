resource "aws_cloudwatch_event_rule" "new_cdm_data_created_for_bulk_load" {
  name        = "data-pipeline-new-cdm-data-created-for-bulk-load"
  description = "Fires when a new source data file is created in ${aws_s3_bucket.cdm_data.bucket} bucket."

  event_pattern = <<PATTERN
{
  "version": ["0"],
  "detail-type": ["Object Created"],
  "source": ["aws.s3"],
  "account": ["${local.account_id}"],
  "region": ["${local.aws_region}"],
  "resources": [
    "${aws_s3_bucket.cdm_data.arn}"
  ],
  "detail": {
    "version": ["0"],
    "bucket": {
      "name": ["${aws_s3_bucket.cdm_data.bucket}"]
    },
    "object": {
      "key": [
        {
          "suffix": ".csv"
        }
      ]
    }
  }
}
PATTERN
}

resource "aws_cloudwatch_event_target" "lambda_bulk_loader" {
  rule      = aws_cloudwatch_event_rule.new_cdm_data_created_for_bulk_load.name
  target_id = "assign-to-lambda-bulk-loader"
  arn       = aws_lambda_function.bulk_loader.arn
  sqs_target {
    
  }
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name",
      key    = "$.detail.object.key"
    }
    input_template = <<EOF
    {
      "bucket": "<bucket>",
      "key": "<key>"
    }
    EOF
  }
}

resource "aws_lambda_permission" "allow_eventbridge_to_invoke_bulk_loader" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.bulk_loader.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.new_cdm_data_created_for_bulk_load.arn
}

resource "aws_cloudwatch_log_group" "eventbridge_log_group_new_cdm_data_created" {
  name = "/aws/events/${aws_cloudwatch_event_rule.new_cdm_data_created_for_bulk_load.name}"
  retention_in_days = 14
}

resource  "aws_cloudwatch_event_target" "log_group_new_cdm_data_created" {
  rule      = aws_cloudwatch_event_rule.new_cdm_data_created_for_bulk_load.name
  target_id = "log_group_new_cdm_data_created"
  arn       = aws_cloudwatch_log_group.eventbridge_log_group_new_cdm_data_created.arn
}