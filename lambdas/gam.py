import json


def process(event, context):
    print("Incoming GAM event:", json.dumps(event))

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "status": "ok",
            "message": "GAM endpoint reached successfully"
        })
    }