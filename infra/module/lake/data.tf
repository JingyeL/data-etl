data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

locals {
    full_name  = var.environment != "" ? "oc-${var.prefix}-${var.name}-${var.environment}" : "oc-${var.prefix}-${var.name}"
    short_name = var.environment != "" ? "${var.name}-${var.environment}" : var.name
    account_name         = var.prefix == "dev" ? "development" : var.prefix == "prod" ? "production" : var.prefix == "play" ? "playground" : var.prefix == "mgmt" ? "management" : var.prefix
    aws_region           = data.aws_region.current.name
    account_id           = data.aws_caller_identity.current.account_id
    default_event_bus_arn = "arn:aws:events:${local.aws_region}:${local.account_id}:event-bus/default"
    glue_version         = "4.0"
    glue_execution_class = "FLEX"
    glue_worker_type     = "G.1X"
    glue_worker_num      = 2                    
    glue_timeout         = 10
}