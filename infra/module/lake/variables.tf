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

variable "config_bucket_name" {
  type        = string
  description = "Name of the S3 bucket stores configuration files/scripts"
}

variable "raw_data_bucket_name" {
  type        = string
  description = "Name of the S3 bucket to store raw data"
}

variable "source_data_bucket_name" {
  type        = string
  description = "Name of the S3 bucket to store source data"
}

variable "cdm_data_bucket_name" {
  type        = string
  description = "Name of the S3 bucket to store CDM data"
}

variable "source_data_event_rule_name" {
  type        = string
  description = "Name of the data event rule the event rule to trigger the glue for source data"
}

variable "cdm_data_event_rule_name" {
  type        = string
  description = "Name of the data event ruleto trigger the glue job for CDM data"
}

variable "lambda_name_glue_service" {
  type        = string
  description = "Name of the lambda function for Glue service"
  default     = "glue_service"
}

variable "trusted_resource_owners" {
  type        = list(string)
  description = "List of IAM ARNs for trusted resource owners"
}

variable "catalog_db_name" {
  type        = string
  description = "Name of the source catalog database. Valid names only contain alphabet characters, numbers and _."
  default = "jingyel_glue_catalog"
}

variable "data_chunk_size" {
  type        = number
  description = "Size of the data chunk in MB"
  default     = 20000 
}

