resource "aws_cloudwatch_event_rule" "event_rule_new_us_fl_historical_raw_data_zip_created" {
  name        = "${local.short_name}-new-historical-raw-data-zip-us-fl"
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
          "wildcard": "us_fl_historical/*.zip"
        }
      ]
    }
  }
}
PATTERN
}

resource "aws_cloudwatch_event_target" "lambda_unzip" {
  rule      = aws_cloudwatch_event_rule.event_rule_new_us_fl_historical_raw_data_zip_created.name
  target_id = "assign-to-lambda-esc-task-handler"
  arn       = aws_lambda_function.ecs_task.arn
  input_transformer {
    input_paths = {
      bucket = "$.detail.bucket.name",
      key    = "$.detail.object.key",
    }
    input_template = <<EOF
    {
      "payload": "{ \"bucket\": \"<bucket>\", \"object_keys\": \"<key>\", \"action\": \"unzip\" }",
      "esc_cluster": "${aws_ecs_cluster.data_etl_ingest_cluster.name}",
      "ecs_task_definition_name": "${aws_ecs_task_definition.data_etl_def.family}",
      "ecs_container": "${local.ecs_name_data_etl_short}",
      "module": "${var.lambda_name_archive_utilty}_handler"
    }
    EOF
  }
}

resource "aws_lambda_permission" "allow_eventbridge_to_invoke_unzip_via_ecs_task" {
  statement_id  = "allow_eventbridge_to_invoke_unzip_via_ecs_task"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ecs_task.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.event_rule_new_us_fl_historical_raw_data_zip_created.arn
}

resource "aws_cloudwatch_log_group" "eventbridge_log_group_new_us_fl_historical_raw_data_zip_created" {
  name = "/aws/events/${aws_cloudwatch_event_rule.event_rule_new_us_fl_historical_raw_data_zip_created.name}"
  retention_in_days = 14
}

resource  "aws_cloudwatch_event_target" "log_group_event_rule_new_us_fl_historical_raw_data_zip_created" {
  rule      = aws_cloudwatch_event_rule.event_rule_new_us_fl_historical_raw_data_zip_created.name
  target_id = "event_rule_new_us_fl_historical_raw_data_zip_created"
  arn       = aws_cloudwatch_log_group.eventbridge_log_group_new_raw_zip_data_created.arn
}