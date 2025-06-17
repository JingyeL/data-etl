resource "aws_db_instance" "datawarehouse" {
  identifier           = var.datawarehouse_db_identifier
  allocated_storage    = 20
  max_allocated_storage = 300
  storage_type         = "gp3"
  engine               = "postgres"
  engine_version       = "16.4"
  instance_class       = "db.t3.medium"
  username             = local.dbuser
  password             = local.dbpassword
  db_name              = var.datawarehouse_db_name
  iam_database_authentication_enabled = false
  skip_final_snapshot  = true
  publicly_accessible  = false
  vpc_security_group_ids = [aws_security_group.rds_psql_sg.id]
  db_subnet_group_name = aws_db_subnet_group.rds_subnet_group.name
  tags = {
    name = var.datawarehouse_db_identifier
  }
}

resource "aws_db_instance_role_association" "rds_role_association" {
  db_instance_identifier = aws_db_instance.datawarehouse.identifier
  role_arn               = aws_iam_role.s3_import_role.arn
  feature_name           = "s3Import"
}

resource "aws_db_subnet_group" "rds_subnet_group_2" {
  name       = "rds-subnet-group-2"
  subnet_ids = var.rds_subnet_ids

  tags = {
    Name = "rds-subnet-group"
  }
}

resource "aws_db_subnet_group" "rds_subnet_group" {
  name       = "rds-subnet-group"
  subnet_ids = var.private_subnet_ids

  tags = {
    Name = "rds-subnet-group"
  }
}

resource "aws_db_proxy" "datawarehouse_proxy" {
  name            = "${aws_db_instance.datawarehouse.identifier}-proxy"
  engine_family   = "POSTGRESQL"
  debug_logging   = false
  idle_client_timeout = 1800
  require_tls     = true
  role_arn        = aws_iam_role.rds_proxy_role.arn
  vpc_subnet_ids  = var.rds_subnet_ids
  vpc_security_group_ids = [aws_security_group.rds_proxy_sg.id]
  auth {  
    auth_scheme = "SECRETS" 
    description = "db secret"
    iam_auth    = "REQUIRED"
    secret_arn  = aws_secretsmanager_secret.datawarehouse_secret.arn
  }
  tags = {
    name = "${aws_db_instance.datawarehouse.id}-proxy"
  }
}

resource "aws_db_proxy_target" "datawarehouse_proxy_target" {
  db_proxy_name         = aws_db_proxy.datawarehouse_proxy.name
  db_instance_identifier = aws_db_instance.datawarehouse.identifier
  target_group_name = aws_db_proxy_default_target_group.this.name
}

resource "aws_db_proxy_default_target_group" "this" {
  db_proxy_name = aws_db_proxy.datawarehouse_proxy.name
  connection_pool_config {
    connection_borrow_timeout    = 120
    init_query                   = "SET x=1, y=2"
    max_connections_percent      = 100
    max_idle_connections_percent = 50
    session_pinning_filters      = ["EXCLUDE_VARIABLE_SETS"]
  }
}