resource "aws_iam_role" "event_notification_role" {
  name = "${local.short_name}-event-notification-role"
  assume_role_policy = data.aws_iam_policy_document.event_notification_instance_assume_role_policy.json
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/CloudWatchEventsBuiltInTargetExecutionAccess",
    "arn:aws:iam::aws:policy/service-role/CloudWatchEventsInvocationAccess"]
}

resource "aws_iam_role_policy_attachment" "event_notification_policy_attachment" {
  role       = aws_iam_role.event_notification_role.name
  policy_arn = aws_iam_policy.event_notification_policy.arn
}

data "aws_iam_policy_document" "event_notification_instance_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
    effect = "Allow"
  }
}

resource "aws_iam_policy" "event_notification_policy" {
  name        = "${local.short_name}-event-notification-policy"
  description = "Policy for event notification"
  policy      = data.aws_iam_policy_document.event_notification_policy_doc.json
}

data "aws_iam_policy_document" "event_notification_policy_doc" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup"
    ]
    resources = [
      "arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:*"
    ]
  }
  statement {
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:${local.aws_region}:${local.account_id}:log-group:/aws/events/*"]
    effect    = "Allow"
  }
}
