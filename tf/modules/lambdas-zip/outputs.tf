# Output the Lambda function ARNs
output "lambda_arns" {
  description = "A map of Lambda function names to their ARNs"
  value = { for key, fn in aws_lambda_function.lambda : key => fn.arn }
}

# Output the S3 paths for Lambda ZIPs
output "lambda_s3_paths" {
  description = "A map of Lambda function names to their S3 object paths"
  value       = { for key, obj in aws_s3_object.lambda_zip : key => obj.key }
}

# Output IAM roles assigned to each Lambda function
output "lambda_iam_roles" {
  description = "A map of Lambda function names to their IAM role ARNs"
  value       = local.effective_lambda_roles
}

# Output CloudWatch log group names
output "lambda_log_groups" {
  description = "A map of Lambda function names to their CloudWatch log groups"
  value       = { for key, log in aws_cloudwatch_log_group.lambda_logging : key => log.name }
}

output "namespace" {
  value = var.namespace
}
