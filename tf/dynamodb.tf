resource "aws_dynamodb_table" "proofing_metadata" {
  name         = "ProofingMetadata"
  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = "workorder_id"
    type = "S"
  }

  hash_key = "workorder_id"

  tags = {
    Name        = "ProofingMetadata"
    Environment = var.env
  }
}
