_bedrock_client = None # cachable across lambda invocations

def get_bedrock_client():
    global _bedrock_client
    if _bedrock_client is None:
        _bedrock_client = boto3.client("bedrock-runtime", region_name="eu-west-2")
    return _bedrock_client
