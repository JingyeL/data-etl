# add config.json to system manager parameter store

resource "aws_ssm_parameter" "cloudwatch_agent_config" {
  name        = "/cloudwatch/agent/config"
  description = "CloudWatch Agent Configuration"
  type        = "String"
  value       = file(var.cloudwatch_agent_config)
}
