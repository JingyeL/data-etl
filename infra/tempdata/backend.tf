terraform {
  backend "s3" {
    bucket = "tmp-data-statefile"
    key    = "oc-jingyel-data-etl-infra/terraform.tfstate"
    region = "eu-west-2"
  }
}
