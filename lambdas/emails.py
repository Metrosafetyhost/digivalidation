import json, time, os, boto3
from botocore.exceptions import ClientError

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Reuse the same constants your live processor uses
# Falls back to literals if the import isn't available in this package
try:
    from salesforce_input import TABLE_NAME as _TABLE_NAME, BUCKET_NAME as _BUCKET_NAME, SENDER as _DEFAULT_SENDER
except Exception:
    _TABLE_NAME = "ProofingMetadata"
    _BUCKET_NAME = "metrosafety-bedrock-output-data-dev-bedrock-lambda"
    _DEFAULT_SENDER = "luke.gasson@metrosafety.co.uk"

TABLE_NAME    = _TABLE_NAME
BUCKET_NAME   = _BUCKET_NAME 
SENDER        = os.getenv("SENDER", _DEFAULT_SENDER)   # optional override via env
REGION        = os.getenv("AWS_REGION", "eu-west-2")
QUIET_SECONDS = int(os.getenv("QUIET_SECONDS", "300"))

dynamodb   = boto3.resource("dynamodb")
s3_client  = boto3.client("s3")
ses_client = boto3.client("ses", region_name=REGION)
table      = dynamodb.Table(TABLE_NAME)

QUIET_SECONDS = int(os.environ.get("QUIET_SECONDS", "300"))

table = dynamodb.Table(TABLE_NAME)

def presign(key: str, expires=3600) -> str:
    return s3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET_NAME, "Key": key},
        ExpiresIn=expires
    )

def send_link_email(to_addr: str, workorder_id: str, url: str):
    subject = f"CSV ready for Work Order {workorder_id}"
    html = f"""
    <p>Hi,</p>
    <p>Your changes CSV for work order <b>{workorder_id}</b> is ready:</p>
    <p><a href="{url}">Download changes CSV</a></p>
    <p>Link expires in 1 hour.</p>
    """
    ses_client.send_email(
        Source=SENDER,
        Destination={"ToAddresses": [to_addr]},
        Message={"Subject": {"Data": subject}, "Body": {"Html": {"Data": html}}}
    )

def process(event, context):
    logger.info(f"Received {len(event.get('Records', []))} SQS record(s)")

    for rec in event["Records"]:
        body = json.loads(rec["body"])
        workorder_id = body.get("workorder_id")
        logger.info(f"Finalizer: workorder_id={workorder_id}")

        item = table.get_item(Key={"workorder_id": workorder_id}).get("Item")
        if not item:
            logger.warning("No DynamoDB item found; skipping")
            continue

        now = int(time.time())
        last_ts = int(item.get("last_event_ts", 0))
        if item.get("emailed") is True:
            logger.info("Already emailed; skipping")
            continue
        if (now - last_ts) < QUIET_SECONDS:
            logger.info(f"Quiet period not met ({now - last_ts}s < {QUIET_SECONDS}s); skipping")
            continue

        key = item.get("changes_s3_key")
        if not key:
            logger.info("No changes_s3_key yet; skipping")
            continue

        url = presign(key)
        to_addr = item.get("emailAddress") or SENDER
        logger.info(f"Sending link to {to_addr}: s3://{BUCKET_NAME}/{key}")

        send_link_email(to_addr, workorder_id, url)

        try:
            table.update_item(
                Key={"workorder_id": workorder_id},
                UpdateExpression="SET emailed = :true, emailed_ts = :now",
                ConditionExpression="attribute_not_exists(emailed) OR emailed = :false",
                ExpressionAttributeValues={":true": True, ":false": False, ":now": now},
            )
            logger.info("Marked emailed=True")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.info("Another worker already marked emailed=True")
            else:
                logger.exception("Failed to update emailed flag")
                raise
