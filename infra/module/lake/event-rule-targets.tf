
# resource "aws_cloudwatch_event_target" "glue_job_source_data" {
#   rule      = var.source_data_event_rule_name
#   target_id = "assign-to-glue-job-source-data"
#   arn       = aws_lambda_function.glue_service.arn
#   input_transformer {
#     input_paths = {
#       bucket = "$.detail.bucket.name",
#       key    = "$.detail.object.key",
#       file_size = "$.detail.object.size"
#     }
#     input_template = <<EOF
#     {
#       "action": "ADD_WORKLOAD",
#       "bucket": "<bucket>",
#       "key": "<key>",
#       "size": "<file_size>",
#       "status": "NEW"
#     }
#     EOF
#   }
# }

# resource "aws_lambda_permission" "allow_eventrule_to_invoke_glue_service_1" {
#   statement_id  = "AllowExecutionFromEventBridge_1"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.glue_service.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = "arn:aws:events:${local.aws_region}:${local.account_id}:rule/${var.source_data_event_rule_name}"
# }


# try add via lambda function
# resource "aws_cloudwatch_event_target" "glue_job_cdm_data" {
#   rule      = var.cdm_data_event_rule_name
#   target_id = "assign-to-glue-job-cdm-data"
#   arn       = aws_lambda_function.glue_service.arn
#   input_transformer {
#     input_paths = {
#       bucket = "$.detail.bucket.name",
#       key    = "$.detail.object.key",
#       file_size = "$.detail.object.size",
#     }
#     input_template = <<EOF
#     {
#       "action": "ADD_WORKLOAD",
#       "bucket": "<bucket>",
#       "key": "<key>",
#       "size": "<file_size>",
#       "status": "NEW"
#     }
#     EOF
#   }
# }

# resource "aws_lambda_permission" "allow_eventbridge_to_invoke_glue_service_2" {
#   statement_id  = "AllowExecutionFromEventBridge"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.glue_service.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = "arn:aws:events:${local.aws_region}:${local.account_id}:rule/${var.cdm_data_event_rule_name}"
# }

