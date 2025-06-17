data "aws_region" "current" {}
data "aws_caller_identity" "current" {}
locals {
  aws_region = data.aws_region.current.name
  account_id = data.aws_caller_identity.current.account_id
}

module "this" {
  source                      = "../module"
  prefix                      = "play"
  environment                 = "play"
  vpc_id                      = "vpc-0a954f2810e40b19a"
  datawarehouse_secret        = "poc/data/open-corporate-data-poc"
  datawarehouse_db_identifier = "open-corporates-data-poc"
  datawarehouse_db_name       = "open_corporates"
  rds_subnet_ids              = ["subnet-0660035e4ffe6ccf2", "subnet-01142e10e8fd525f1", "subnet-0304ec23dcc48ec47"]
  public_subnet_ids           = ["subnet-0b2756444891ad3f8", "subnet-05674c92714e6e2a4", "subnet-0674e676e31cb0870"]
  private_subnet_ids          = ["subnet-00bb6609cdeecffb6", "subnet-052e40015bc0e3208", "subnet-0f5858d8a58c1d098"]
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
  sso_role_tf_exec        = "AWSReservedSSO_Data-Engineer_Elevated_ef288a0bec562eba"
  vpn_cidr                = "10.99.0.0/16"
  data_chunk_size         = 10000
}

module "data_lake" {
  source                      = "../module/lake"
  prefix                      = "play"
  environment                 = "play"
  raw_data_bucket_name        = module.this.s3_raw_data_bucket_name
  source_data_bucket_name     = module.this.s3_source_data_bucket_name
  cdm_data_bucket_name        = module.this.s3_cdm_data_bucket_name
  config_bucket_name          = module.this.s3_config_bucket_name
  source_data_event_rule_name = module.this.new_source_data_event_rule_name
  cdm_data_event_rule_name    = module.this.new_cdm_data_event_rule_name
  trusted_resource_owners     = [local.account_id]
  data_chunk_size             = 20000
}
