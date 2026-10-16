import os

# If OS ENV is nothing defaults too ProofingMetdata aka you can override table name by using ENV variables
def get_table_name():
    return os.getenv("TABLE_NAME", "ProofingMetadata")

def get_bedrock_model_id():
    return os.getenv("BEDROCK_MODEL_ID", "eu.anthropic.claude-sonnet-5")

def get_bucket_name():
    return os.getenv("BUCKET_NAME", "metrosafety-bedrock-output-data-dev-bedrock-lambda")
