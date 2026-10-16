locals {
  # EU cross-Region inference requires the profile and every destination model.
  sonnet_invoke_arns = concat(
    ["arn:aws:bedrock:eu-west-2:${data.aws_caller_identity.current.account_id}:inference-profile/eu.anthropic.claude-sonnet-5"],
    [for region in ["eu-central-1", "eu-north-1", "eu-south-1", "eu-south-2", "eu-west-1", "eu-west-2", "eu-west-3"] :
      "arn:aws:bedrock:${region}::foundation-model/anthropic.claude-sonnet-5"
    ]
  )
  sonnet_model_arn = "arn:aws:bedrock:eu-west-2::foundation-model/anthropic.claude-sonnet-5"

  common_tags = {
    ManagedBy = "terraform"
    git_repo  = var.repo_name
    git_org   = var.gh_org
  }
}
