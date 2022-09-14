terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-east-1"
  profile = "udacity_de_l5"
}

resource "aws_emr_cluster" "spark-cluster" {
  name          = "spark-cluster2"
  release_label = "emr-5.36.0"
  applications  = ["Hadoop", "Spark", "JupyterEnterpriseGateway",
    "Livy", "Hive"]
  
  ec2_attributes {
    subnet_id = "subnet-09bd36beeb2ad646b"
    key_name = "spark-cluster"
    emr_managed_master_security_group = "sg-XXXXXXXXXXXXXXXXX"
    emr_managed_slave_security_group  = "sg-YYYYYYYYYYYYYYYYY"
    instance_profile                  = "EMR_EC2_DefaultRole"
  }
  
  master_instance_group {
    instance_type = "m3.xlarge"
    instance_count = 1
  }
  
  core_instance_group {
    instance_type = "m3.xlarge"
    instance_count = 2
  }

  service_role = "EMR_DefaultRole"
}

