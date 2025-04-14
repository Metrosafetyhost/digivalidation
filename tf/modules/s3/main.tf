resource "aws_s3_bucket" "this" {

  bucket        = var.bucket
  bucket_prefix = var.bucket_prefix
  tags = merge(var.common_tags, {
    git_file = "s3/main.tf"
  })
  force_destroy       = var.force_destroy
  acceleration_status = var.acceleration_status
  request_payer       = var.request_payer

}

resource "aws_s3_bucket_acl" "this" {
  count  = var.enable_acl ? 1 : 0
  bucket = aws_s3_bucket.this.id
  acl    = var.acl
}

resource "aws_s3_bucket_website_configuration" "this" {
  count = length(keys(var.website)) == 0 ? 0 : 1

  bucket = aws_s3_bucket.this.id

  index_document {
    suffix = lookup(var.website, "index_document", null)
  }

  error_document {
    key = lookup(var.website, "error_document", null)
  }

  routing_rules = lookup(var.website, "routing_rules", null)

  dynamic "redirect_all_requests_to" {
    for_each = lookup(var.website, "redirect_all_requests_to", [])
    content {
      host_name = lookup(redirect_all_requests_to.value, "host_name", null)
      protocol  = lookup(redirect_all_requests_to.value, "protocol", null)
    }
  }
}

resource "aws_s3_bucket_cors_configuration" "this" {
  count = length(var.cors_rule) > 1 ? 1 : 0

  bucket = aws_s3_bucket.this.id

  dynamic "cors_rule" {
    for_each = var.cors_rule
    content {
      allowed_methods = cors_rule.value.allowed_methods
      allowed_origins = cors_rule.value.allowed_origins
      allowed_headers = lookup(cors_rule.value, "allowed_headers", null)
      expose_headers  = lookup(cors_rule.value, "expose_headers", null)
      max_age_seconds = lookup(cors_rule.value, "max_age_seconds", null)
    }
  }
}

resource "aws_s3_bucket_versioning" "this" {
  count = length(keys(var.versioning)) == 0 ? 0 : 1

  bucket = aws_s3_bucket.this.id

  versioning_configuration {
    status     = lookup(var.versioning, "enabled", null)
    mfa_delete = lookup(var.versioning, "mfa_delete", null)
  }
}

resource "aws_s3_bucket_logging" "this" {
  count = length(keys(var.logging)) == 0 ? 0 : 1

  bucket = aws_s3_bucket.this.id

  target_bucket = var.logging.target_bucket
  target_prefix = lookup(var.logging, "target_prefix", null)
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  count  = var.encrypt_bucket ? 1 : 0
  bucket = aws_s3_bucket.this.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_id != "" ? "aws:kms" : "AES256"
      kms_master_key_id = var.kms_key_id
    }
  }
}

# Max 1 block - object_lock_configuration
resource "aws_s3_bucket_object_lock_configuration" "this" {
  count = length(keys(var.object_lock_configuration)) == 0 ? 0 : 1

  bucket = aws_s3_bucket.this.id

  object_lock_enabled = var.object_lock_configuration.object_lock_enabled

  dynamic "rule" {
    for_each = length(keys(lookup(var.object_lock_configuration, "rule", {}))) == 0 ? [] : [lookup(var.object_lock_configuration, "rule", {})]

    content {
      default_retention {
        mode  = lookup(lookup(rule.value, "default_retention", {}), "mode")
        days  = lookup(lookup(rule.value, "default_retention", {}), "days", null)
        years = lookup(lookup(rule.value, "default_retention", {}), "years", null)
      }
    }
  }
}

resource "aws_s3_bucket_policy" "this" {
  bucket = aws_s3_bucket.this.id
  policy = data.aws_iam_policy_document.final_policy_doc.json
}

data "aws_iam_policy_document" "final_policy_doc" {
  source_policy_documents = [
    data.aws_iam_policy_document.base_policy_doc.json,
    var.policy
  ]
}

data "aws_iam_policy_document" "base_policy_doc" {
  statement {
    sid = "RequireTLS"

    effect = "Deny"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      "arn:aws:s3:::${var.bucket}",
      "arn:aws:s3:::${var.bucket}/*"
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]

    }
  }
  statement {
    sid = "RequireTLS12"

    effect = "Deny"

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      "arn:aws:s3:::${var.bucket}",
      "arn:aws:s3:::${var.bucket}/*"
    ]

    condition {
      test     = "NumericLessThan"
      variable = "s3:TlsVersion"
      values   = ["1.2"]
    }
  }
}

output "https_deny_policy" {
  value = data.aws_iam_policy_document.base_policy_doc.json
}

resource "aws_s3_bucket_public_access_block" "this" {
  bucket = aws_s3_bucket.this.id

  block_public_acls       = var.block_public_acls
  block_public_policy     = var.block_public_policy
  ignore_public_acls      = var.ignore_public_acls
  restrict_public_buckets = var.restrict_public_buckets
}

resource "aws_s3_bucket_ownership_controls" "this" {
  bucket = aws_s3_bucket.this.id
  rule {
    object_ownership = var.object_ownership
  }
}

resource "aws_ssm_parameter" "s3_bucket_ssm" {
  count = var.create_ssm ? 1 : 0
  name  = "/kapua-core/${var.env}/${var.bucket}"
  type  = "String"
  value = aws_s3_bucket.this.bucket
  tags = merge(var.common_tags, {
    git_file = "s3/main.tf"
  })
}

output "this_s3_bucket_resource" {
  value = aws_s3_bucket.this
}

#folder structrue for bedrock output 
variable "s3_folder_structure" {
  default = ["original", "proofed"]
}

resource "aws_s3_object" "create_folders" {
  for_each = toset(var.s3_folder_structure)

  bucket = aws_s3_bucket.this.id
  key    = "${each.value}/"  # creats empty folders
}

resource "aws_s3_bucket" "textract_output" {
  bucket = "textract-output"

  tags = merge(var.common_tags, {
    Name    = "Textract Output Bucket",
    git_file = "s3/main.tf"
  })
}