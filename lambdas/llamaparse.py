import os, json
from llama_parse import LlamaParse
from llama_index.readers.s3 import S3Reader

def process(event, context):
    bucket = event["bucket"]
    key    = event["key"]
    region = os.getenv("AWS_REGION", "eu-west-2")

    api_key = os.getenv("LLAMA_CLOUD_API_KEY")
    if not api_key:
        raise RuntimeError("LLAMA_CLOUD_API_KEY is not set")

    parser = LlamaParse(api_key=api_key, result_type="markdown")
    docs = S3Reader(
        bucket=bucket,
        key=key,
        region_name=region,
        file_extractor={".pdf": parser},
    ).load_data()

    if not docs:
        return {"statusCode": 404, "body": json.dumps({"msg": "no document loaded"})}

    return {
        "statusCode": 200,
        "body": json.dumps({
            "s3": f"s3://{bucket}/{key}",
            "snippet": docs[0].text[:500]
        })
    }
