resource "aws_cloudwatch_event_rule" "schedule_sftp_fetch_new_workload" {
    name        = "data-pipeline-schedule_sftp_fetch_new_workload"
    description = "Fires on scheduled time, add new raw data file job to ${aws_dynamodb_table.etl_job_register.name}."
    schedule_expression = "cron(0 5 * * ? *)"
}

resource "aws_cloudwatch_event_target" "lambda_shedule_sftp_fetch_new_workload" {
    rule      = aws_cloudwatch_event_rule.schedule_sftp_fetch_new_workload.name
    target_id = "fetcher_ftp"
    arn       = aws_lambda_function.ingestion_services.arn
    input_transformer {
      input_template = <<EOF
      {
        "action": "ADD_WORKLOAD"
      }
      EOF
  }
  
}

resource "aws_lambda_permission" "allow_eventbridge_to_invoke_fetcher_ftp_1" {
    statement_id  = "AllowExecutionFromCloudWatch_1"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.ingestion_services.function_name
    principal     = "events.amazonaws.com"
    source_arn    = aws_cloudwatch_event_rule.schedule_sftp_fetch_new_workload.arn
}

resource "aws_cloudwatch_event_rule" "schedule_sftp_fetch" {
    name        = "data-pipeline-schedule_sftp_fetch"
    description = "Fires on scheduled time, download raw data file to ${aws_s3_bucket.raw_data.bucket} bucket."
    schedule_expression = "cron(0 6 * * ? *)"
}

resource "aws_cloudwatch_event_target" "lambda_shedule_sftp_fetch" {
    rule      = aws_cloudwatch_event_rule.schedule_sftp_fetch.name
    target_id = "ingestion_services"
    arn       = aws_lambda_function.ingestion_services.arn
  input_transformer {

    input_template = <<EOF
    {
      "action": "START"
    }
    EOF
  }

}

resource "aws_lambda_permission" "allow_eventbridge_to_invoke_fetcher_ftp_2" {
    statement_id  = "AllowExecutionFromCloudWatch_2"
    action        = "lambda:InvokeFunction"
    function_name = aws_lambda_function.ingestion_services.function_name
    principal     = "events.amazonaws.com"
    source_arn    = aws_cloudwatch_event_rule.schedule_sftp_fetch.arn
}

resource "aws_cloudwatch_log_group" "eventbridge_log_group_new_fetch" {
  name = "/aws/events/${aws_cloudwatch_event_rule.schedule_sftp_fetch.name}"
  retention_in_days = 14
}

resource  "aws_cloudwatch_event_target" "log_group_new_fetch" {
  rule      = aws_cloudwatch_event_rule.schedule_sftp_fetch.name
  target_id = "log_group_new_fetch"
  arn       = aws_cloudwatch_log_group.eventbridge_log_group_new_fetch.arn
}