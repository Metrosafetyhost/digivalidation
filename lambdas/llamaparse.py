# llamaparse.py (handler = "process")
import os, json, boto3, tempfile
from llama_parse import LlamaParse

s3 = boto3.client("s3")

def process(event, context):
    bucket = event["bucket"]
    key    = event["key"]

    # Download to /tmp
    fd, tmp_path = tempfile.mkstemp(suffix=os.path.splitext(key)[1] or ".pdf")
    os.close(fd)
    s3.download_file(bucket, key, tmp_path)

    parser = LlamaParse(
        api_key=os.environ["LLAMA_CLOUD_API_KEY"],
        result_type="markdown"
    )
    docs = parser.load_data(tmp_path)  # returns List[Document]
    if not docs:
        return {"statusCode": 404, "body": json.dumps({"msg": "no document loaded"})}

    text = docs[0].text or ""
    return {
        "statusCode": 200,
        "body": json.dumps({
            "s3": f"s3://{bucket}/{key}",
            "snippet": text[:500]
        })
    }
