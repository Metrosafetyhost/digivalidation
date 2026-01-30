# # DEWRRA Async Job Infrastructure
# # - HTTP API endpoints: /dewrra/start, /status/{jobId}, /results/{jobId}
# # - DynamoDB job tracking
# # - SQS-backed worker lambda
# # Replaces legacy synchronous /pdf_qa usage for Salesforce


# #Dynamo DB table 
# resource "aws_dynamodb_table" "dewrra_jobs" {
#   name         = "dewrra_jobs"
#   billing_mode = "PAY_PER_REQUEST"
#   hash_key     = "jobId"

#   attribute {
#     name = "jobId"
#     type = "S"
#   }
# }

# #SQS Queue
# resource "aws_sqs_queue" "dewrra_jobs_dlq" {
#   name = "dewrra_jobs_dlq"
# }

# resource "aws_sqs_queue" "dewrra_jobs_queue" {
#   name = "dewrra_jobs_queue"

#   redrive_policy = jsonencode({
#     deadLetterTargetArn = aws_sqs_queue.dewrra_jobs_dlq.arn
#     maxReceiveCount     = 3
#   })
# }

# #Lambda Trigger
# resource "aws_lambda_event_source_mapping" "dewrra_worker_sqs" {
#   event_source_arn = aws_sqs_queue.dewrra_jobs_queue.arn
#   function_name    = "bedrock-lambda-dewrra-worker"
#   batch_size       = 1
# }

