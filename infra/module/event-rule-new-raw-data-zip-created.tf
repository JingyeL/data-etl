resource "aws_cloudwatch_event_rule" "s3_new_raw_data_zip_created" {
  name        = "data-pipeline-s3-new-raw-data-zip-created"
  description = "Fires when new zip raw data files are created in ${aws_s3_bucket.raw_data.bucket} bucket."

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
          "prefix": "im/"
        }
      ]
    }
  }
}
PATTERN
}

# resource "aws_cloudwatch_event_target" "lambda_im_parser" {
#   rule      = aws_cloudwatch_event_rule.s3_new_raw_data_zip_created.name
#   target_id = "assign-to-lambda-parser"
#   arn       = "arn:aws:lambda:eu-west-2:990229050728:function:im-parser"
#   input_transformer {
#     input_paths = {
#       bucket = "$.detail.bucket.name",
#       key    = "$.detail.object.key",
#     }
#     input_template = <<EOF
#     {
#       "bucket": "<bucket>",
#       "key": "<key>"
#     }
#     EOF
#   }
# }

# resource "aws_lambda_permission" "allow_eventbridge_to_invoke_im_parser" {
#   statement_id  = "AllowExecutionFromEventBridge"
#   action        = "lambda:InvokeFunction"
#   function_name = "im-parser"
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.s3_new_raw_data_zip_created.arn
# }

resource "aws_cloudwatch_log_group" "eventbridge_log_group_new_raw_zip_data_created" {
  name = "/aws/events/${aws_cloudwatch_event_rule.s3_new_raw_data_zip_created.name}"
  retention_in_days = 14
}

resource  "aws_cloudwatch_event_target" "log_group_new_raw_zip_data_created" {
  rule      = aws_cloudwatch_event_rule.s3_new_raw_data_zip_created.name
  target_id = "log_group_new_raw_zip_data_created"
  arn       = aws_cloudwatch_log_group.eventbridge_log_group_new_raw_zip_data_created.arn
}