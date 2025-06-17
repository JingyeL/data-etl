variable "prefix" {
  type        = string
  description = "Prefix for the environment"
  nullable    = false

  validation {
    condition     = contains(["dev", "prod", "play", "audit", "idp", "mgmt", "tmpdata"], var.prefix)
    error_message = "Invalid value for variable: prefix please use either: dev, prod, play, audit, idp or mgmt."
  }
}

variable "name" {
  description = "project name"
  type        = string
  default     = "data-pipeline"
}

variable "environment" {
  description = "Name of the environment"
  type        = string
}

variable "vpc_id" {
  type        = string
  description = "ID of the VPC"

}

variable "datawarehouse_secret" {
  type        = string
  description = "Name of the secret containing the datawarehouse credentials"
}

variable "datawarehouse_db_identifier" {
  type        = string
  description = "Name of the database instance"
}

variable "datawarehouse_db_name" {
  type        = string
  description = "Name of the database"
}

variable "rds_subnet_ids" {
  type        = list(string)
  description = "List of subnet IDs for RDS"

}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Temp List of subnet IDs for RDS"
}

variable "public_subnet_ids" {
  type        = list(string)
  description = "List of subnet IDs for public access"
}

variable "lambda_name_bulk_loader" {
  type        = string
  description = "Name of the lambda function for bulk loading"
  default     = "data_bulk_load"
}

variable "lambda_name_schema_transformation" {
  type        = string
  description = "Name of the lambda function for schema mapping"
  default     = "schema_transformation"
}

variable "lambda_name_fixed_width_parser" {
  type        = string
  description = "Name of the lambda function for fixed width raw data parsing"
  default     = "fixed_width_txt"
}

variable "lambda_name_backfill_file_meta" {
  type        = string
  description = "Name of the lambda function for backfilling file metadata"
  default     = "backfill_file_meta"
}

variable "lambda_name_update_meta_ts" {
  type        = string
  description = "Name of the lambda function for updating metadata timestamp fields"
  default     = "update_metadata_timestamp"
}

variable "memory_size_etl" {
  type        = number
  description = "Memory size for handling ETL tasks"
}

variable "memory_size_service" {
  type        = number
  description = "Memory size for handling ETL services"
}

variable "lambda_name_ingestion_services" {
  type        = string
  description = "Name of the lambda function for managing ingestion_services"
  default     = "ingestion_services"
}

variable "lambda_name_fetcher_ftp" {
  type        = string
  description = "Name of the lambda function for fetching data from FTP"
  default     = "fetcher_ftp"
}

variable "lambda_name_fetcher_ftp_download" {
  type        = string
  description = "Name of the lambda function for downloading data from FTP"
  default     = "fetcher_ftp_download"
}

variable "ecs_name_data_etl" {
  type        = string
  description = "esc task name for data etl"
  default     = "ecs_data_etl"
}

variable "lambda_name_archive_utilty" {
  type        = string
  description = "Name of the lambda function for creating and extracting archive"
  default     = "archive_utility"
}

variable "lambda_name_ecs_task" {
  type        = string
  description = "Name of the lambda function to create ecs task"
  default     = "ecs_task"
}

variable "ftp_fetching_max" {
  type        = number
  description = "Maximum number of files to fetch from an FTP/SFTP server a time"
  default     = 20
}

variable "config_files" {
  type        = map(string)
  description = "Configuration files for the data pipeline"
}

variable "s3_vpc_endpoint_sg_id" {
  type        = string
  description = "ID of the security group for aws s3 vpc endpoint"
  default     = "com.amazonaws.region.s3.pl-7ca54015"
}

variable "sftp_secret_us_fl" {
  type        = string
  description = "Name of the secret containing the SFTP credentials for US-FL"
  default     = "poc/data/sftp/us_fl"
}

variable "max_fetching_workers" {
  type        = number
  description = "Maximum number of workers for fetching data from FTP/SFTP"
  default     = 3
}

variable "cloudwatch_agent_config" {
  type        = string
  description = "Path to the CloudWatch agent configuration file"
}

variable "sso_role_tf_exec" {
  type        = string
  description = "Name of the role to assume for Terraform execution"
}

variable "vpn_cidr" {
  type        = string
  description = "CIDR block for the VPN, for example 10.99.0.0/16"
}

variable "fixed_width_txt_raw_data_prefixes" {
  type        = list(string)
  description = "Prefixes for the fixed width txt raw data files"
  default     = ["us_fl/", "us_fl_historical/"]
}

variable "fixed_width_txt_raw_data_suffixes" {
  type        = string
  description = "Suffixes for the fixed width txt raw data files"
  default     = ".txt"
}

variable "data_chunk_size" {
  type        = number
  description = "Size of the data chunk in MB"
  default     = 20000 
}