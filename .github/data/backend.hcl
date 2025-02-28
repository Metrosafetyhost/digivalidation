    bucket         = "metrosafety-cloud-env-tf-state-master"
    key            = "{{ SERVICENAME }}/terraform.{{ ENV }}.tfstate"
    region         = "eu-west-2"
    dynamodb_table = "terraform-state-locks"
