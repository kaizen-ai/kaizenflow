terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
  backend "s3" {
    bucket         = "cryptokaizen-airflow-backend-state"
    key            = "airflow/terraform.tfstate"
    region         = "eu-north-1"
    dynamodb_table = "cryptokaizen_airflow_backend_lock"
    encrypt        = true
    profile        = "ck"
  }
}

provider "aws" {
  region  = var.region
  profile = var.profile
}
