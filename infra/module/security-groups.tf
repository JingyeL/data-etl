resource "aws_security_group" "lambda_sg" {
  name        = "lambda-sg"
  description = "Security group for Lambda functions"
  vpc_id      = var.vpc_id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "rds_proxy_sg" {
  name        = "rds-proxy-sg"
  description = "Security group for RDS Proxy"
  vpc_id      = var.vpc_id
}

resource "aws_security_group_rule" "rds_proxy_sg_ingress" {
  type              = "ingress"
  from_port         = 5432
  to_port           = 5432
  protocol          = "tcp"
  security_group_id = aws_security_group.rds_proxy_sg.id
  source_security_group_id = aws_security_group.lambda_sg.id
  description       = "allow Lambda function security groups to connect to the RDS Proxy"
}

resource "aws_security_group_rule" "rds_proxy_sg_egress_rds_psql" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.rds_proxy_sg.id
  source_security_group_id = aws_security_group.rds_psql_sg.id
  description       = "allow outbound traffic to rds psql"
}

resource "aws_security_group_rule" "rds_proxy_sg_egress_lambda" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.rds_proxy_sg.id
  source_security_group_id = aws_security_group.lambda_sg.id
  description       = "allow outbound traffic to lambda"
}
data "aws_prefix_list" "s3" {
  name = "com.amazonaws.${local.aws_region}.s3"
}

resource "aws_security_group" "rds_psql_sg" {
  name        = "rds-psql-sg"
  description = "Security group for RDS Postgres instance"
  vpc_id      = var.vpc_id

    ingress {
        from_port   = 5432
        to_port     = 5432
        protocol    = "tcp"
        cidr_blocks = [var.vpn_cidr]
        description = "Allows inbound traffic from the VPN"
    }

    ingress {
      from_port   = 5432
      to_port     = 5432
      protocol    = "tcp"
      cidr_blocks = data.aws_prefix_list.s3.cidr_blocks
      description = "Allows inbound HTTPS traffic from the S3 prefix list"
    }

    ingress {
        from_port   = 5432
        to_port     = 5432
        protocol    = "tcp"
        security_groups = [aws_security_group.rds_proxy_sg.id]
        description = "Allows inbound traffic from the RDS Proxy security group"
    }

    ingress{
        from_port   = 5432
        to_port     = 5432
        protocol    = "tcp"
        security_groups = [aws_security_group.lambda_sg.id]
        description = "Allows inbound traffic from the Lambda security group"
    }

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
        description = "Allows outbound traffic to the internet"
    }
}

resource "aws_security_group" "ecs_security_group" {
  name        = "ecs-security-group"
  description = "Security group for ECS tasks"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allows all outbound traffic"
  }
  
  tags = {
    Name = "ecs-security-group"
  }
}