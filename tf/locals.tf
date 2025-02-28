locals {
  common_tags = {
    ManagedBy = "terraform"
    git_repo  = var.repo_name
    git_org   = var.gh_org
  }
}
