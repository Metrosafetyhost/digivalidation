# Metrosafety Kapua Lambda Template

This repo is to be used as a template for provisioning AWS lambdas. It provides
a Makefile like interface where the lambda layer and zips are build locally
and terraform `plan` can be run locally to test the infrastructure build.
Terraform `apply`  can only be done via Github CICD but all tests and plan can be done locally.

## Bootstrap new project
To configure a new project - clone this repo https://github.com/Metrosafetyhost/kapua-py-lambdas-template
, install bb (see below) and then run `bb -BOOTSTRAP-PROJECT` to change

##  Install babashka and python package manager uv
It is using a Makefile derivative called babashka (command-line is `bb`) which helps build the
lambdas and the AWS layer before calling terraform plan or apply.
this can be installed on your Mac with the script locatd at `bin\install-cicd-tool.sh`

The package installer is using the fast `uv` command which is also installed with the bin script

## Terraform files
are located in the `tf` directory. This template will build your lambda layer zip and your zips for any python
lambdas under the `lambdas` directory. The zip files will be staged on an S3 bucket ala CDK. This build process
will run automatically on `bb plan` or `bb apply`. They can be run manually by typing:
- `bb -build-layer` or
- `bb -build-lambdas`

## Local Development
Using the `bb` Makefile you can test your lambdas and then deploy if you have the correct AWS permissions
(refer  `assume -e`)

Here are the commands you have available from the build-tool
``` bb tasks
The following tasks are available:

check-aws Check AWS session status.
format    Format python lambdas files.
test      Run local python runtime tests.
init      Run Terraform Init Locally
refresh   Sync Terraform state with Refresh.
validate  Run Terraform validate.
plan      Run Terraform plan.
apply     Run Terraform apply.
```

So to do a terraform plan from the root directory type:
`bb plan`
