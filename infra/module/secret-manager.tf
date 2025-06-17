resource "aws_secretsmanager_secret" "datawarehouse_secret" {
  name = var.datawarehouse_secret
  kms_key_id = aws_kms_key.sops.key_id
}

resource "aws_secretsmanager_secret_version" "db_secret_version" {
  secret_id = aws_secretsmanager_secret.datawarehouse_secret.id
  secret_string = jsonencode({
    username = local.dbuser
    password = local.dbpassword
    dbname   = var.datawarehouse_db_name
    host     = aws_db_instance.datawarehouse.address
    port     = aws_db_instance.datawarehouse.port
  })
}

resource "aws_secretsmanager_secret" "sftp_secret_us_fl" {
  name = var.sftp_secret_us_fl
  kms_key_id = aws_kms_key.sops.key_id
}

resource "aws_secretsmanager_secret_version" "sftp_secret_us_fl_version" {
  secret_id = aws_secretsmanager_secret.sftp_secret_us_fl.id
  secret_string = jsonencode({
    username = data.sops_file.secrets.data["sftp.us_fl.username"]
    password = data.sops_file.secrets.data["sftp.us_fl.password"]
    host     = data.sops_file.secrets.data["sftp.us_fl.host"]
    port     = data.sops_file.secrets.data["sftp.us_fl.port"]
  })
}