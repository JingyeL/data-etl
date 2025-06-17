resource "aws_cloudwatch_event_rule" "schedule_glue_service" {
    name        = "data-pipeline-schedule-glue-service"
    description = "Fires on every hour,except weekends, to trigger the glue service lambda function."
    schedule_expression = "cron(0 * ? * MON-FRI *)"
}

resource "aws_cloudwatch_event_target" "lambda_glue_service_target" {
    rule      = aws_cloudwatch_event_rule.schedule_glue_service.name
    target_id = "glue_service"
    arn       = aws_lambda_function.glue_service.arn

  input_transformer {

    input_template = <<EOF
    {
      "action": "START",
      "DYNAMODB_TABLE": "${aws_dynamodb_table.job_register.name}"

    }
    EOF
  }
}

resource "aws_lambda_permission" "allow_eventbridge_to_invoke_glue_service" {
    statement_id  = "AllowExecutionFromCloudWatch"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.glue_service.function_name
    principal     = "events.amazonaws.com"
    source_arn    = aws_cloudwatch_event_rule.schedule_glue_service.arn
}

resource "aws_cloudwatch_log_group" "eventbridge_log_group_glue_service" {
    name = "/aws/events/${aws_cloudwatch_event_rule.schedule_glue_service.name}"
    retention_in_days = 14
}

resource  "aws_cloudwatch_event_target" "log_group_schedule_glue_service" {
  rule      = aws_cloudwatch_event_rule.schedule_glue_service.name
  target_id = "log_group_glue_service"
  arn       = aws_cloudwatch_log_group.eventbridge_log_group_glue_service.arn
}