    bucket         = "trove-cloud-env-terraform-state-{{ ENV }}"
    key            = "{{ SERVICENAME }}/terraform.{{ ENV }}.tfstate"
    region         = "ap-southeast-2"
    dynamodb_table = "terraform-state"
