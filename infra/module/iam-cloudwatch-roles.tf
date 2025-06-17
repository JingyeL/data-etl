resource "aws_iam_role" "cloudwatch_agent_role" {
  name        = "${local.short_name}-cloudwatch-agent-role"
  description = "IAM role attaches to each EC2 instance that runs the CloudWatch agent. This role provides permissions for reading information from the instance and writing it to CloudWatch. "
  managed_policy_arns = ["arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy",
  "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore", ]
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
    }
  )

  tags = {
    Name = "${local.short_name}-cloudwatch-agent-role"
  }
}
