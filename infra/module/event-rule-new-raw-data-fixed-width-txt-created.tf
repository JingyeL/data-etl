resource "aws_cloudwatch_event_rule" "s3_new_raw_data_fixed_width_txt_created" {
  name        = "data-pipeline-s3_new_fixed_width_txt_raw_data_created"
  description = "Fires when new fixed width txt raw data files are created in ${aws_s3_bucket.raw_data.bucket} bucket."

  event_pattern = <<PATTERN
{
  "version": ["0"],
  "detail-type": ["Object Created"],
  "source": ["aws.s3"],
  "account": ["${local.account_id}"],
  "region": ["${local.aws_region}"],
  "resources": [
    "${aws_s3_bucket.raw_data.arn}"
  ],
  "detail": {
    "version": ["0"],
    "bucket": {
      "name": ["${aws_s3_bucket.raw_data.bucket}"]
    },
    "object": {
      "key": [
        {
          "suffix": ".txt"
        }
      ]
    }
  }
}
PATTERN
}

resource "aws_cloudwatch_event_target" "lambda_parser" {
  rule      = aws_cloudwatch_event_rule.s3_new_raw_data_fixed_width_txt_created.name
  target_id = "assign-to-lambda-parser"
  arn       = aws_lambda_function.fixed_width_parser.arn
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name",
      key    = "$.detail.object.key",
    }
    input_template = <<EOF
    {
      "bucket": "<bucket>",
      "key": "<key>",
      "action": "START",
      "chunk_size": "0"
    }
    EOF
  }
}

resource "aws_lambda_permission" "allow_eventbridge_to_invoke_fwt_parser" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fixed_width_parser.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.s3_new_raw_data_fixed_width_txt_created.arn
}

resource "aws_cloudwatch_log_group" "eventbridge_log_group_new_raw_fwt_data_created" {
  name = "/aws/events/${aws_cloudwatch_event_rule.s3_new_raw_data_fixed_width_txt_created.name}"
  retention_in_days = 14
}

resource  "aws_cloudwatch_event_target" "log_group_new_raw_fwt_data_created" {
  rule      = aws_cloudwatch_event_rule.s3_new_raw_data_fixed_width_txt_created.name
  target_id = "log_group_new_raw_fwt_data_created"
  arn       = aws_cloudwatch_log_group.eventbridge_log_group_new_raw_fwt_data_created.arn
}