terraform {  
  required_version = ">= 1.1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.49.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.0"
    }
  }
}
  
  provider "aws" {
  region = "eu-west-2"

  default_tags {
    tags = {
      terraform           = "true"
      terraform_workspace = "infra/development"
      system              = "new-data-platform"
      domain              = "poc"
    }
  }
}