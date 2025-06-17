data "aws_region" "current" {}
data "aws_caller_identity" "current" {}
locals {
  aws_region = data.aws_region.current.name
  account_id = data.aws_caller_identity.current.account_id
}

module "this" {
  source                      = "../module"
  prefix                      = "sandbox"
  environment                 = "sandbox"
  vpc_id                      = "vpc-abcd1234567901234"
  datawarehouse_secret        = "jingyel/data/jingyel-data-etl"
  datawarehouse_db_identifier = "jingyel-data-etl-db"
  datawarehouse_db_name       = "data-etl"
  rds_subnet_ids              = ["subnet-1", "subnet-2", "subnet-3"]
  public_subnet_ids           = ["subnet-pub-1", "subnet-pub-2", "subnet-pub-3"]
  private_subnet_ids          = ["subnet-pri-1", "subnet-pri-2", "subnet-pri-3"]
  memory_size_etl             = 2048
  memory_size_service         = 256
  config_files = {
    "cdm_mapping_rules/im/jsonlines/latest/cdm_mapping_im.json" = "../config/cdm_mapping_rules/cdm_mapping_im.json",
    "cdm_mapping_rules/us_fl/csv/latest/cdm_mapping_us_fl.json" = "../config/cdm_mapping_rules/cdm_mapping_us_fl.json",
    "fixed_width_field_def/us_fl//latest/us_fl.json"            = "../config/fixed_width_field_def/us_fl.json",
    "source_ingestion/ingestion_meta_data.json"                 = "../config/source_ingestion/ingestion_meta_data.json"
    "test_folder/t1.csv"                                        = "../config/test_folder/t1.csv"
  }
  max_fetching_workers    = 2
  cloudwatch_agent_config = "../config/cloudwatch-agent-config.json"
  sso_role_tf_exec        = "AWSReservedSSO_Data-Engineer_Elevated_5568e75b6ecc4827"
  vpn_cidr                = "10.98.0.0/16"
}

module "data_lake" {
  source                      = "../module/lake"
  prefix                      = "sandbox"
  environment                 = "sandbox"
  raw_data_bucket_name        = module.this.s3_raw_data_bucket_name
  source_data_bucket_name     = module.this.s3_source_data_bucket_name
  cdm_data_bucket_name        = module.this.s3_cdm_data_bucket_name
  config_bucket_name          = module.this.s3_config_bucket_name
  source_data_event_rule_name = module.this.new_source_data_event_rule_name
  cdm_data_event_rule_name    = module.this.new_cdm_data_event_rule_name
  trusted_resource_owners     = [local.account_id]
}
