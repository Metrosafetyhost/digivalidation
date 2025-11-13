import json

def process(event, context):
    print("Incoming event:", json.dumps(event))

    # Try to parse the incoming JSON body
    try:
        body = json.loads(event.get("body", "{}"))
    except Exception as e:
        print("JSON parse error:", str(e))
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Invalid JSON"})
        }

    # Log parsed JSON
    print("Parsed JSON:", json.dumps(body))

    # Extract fields (they may or may not exist)
    work_order_id = body.get("workOrderId")
    cases = body.get("cases", [])
    case_count = body.get("caseCount", len(cases))

    # Your processing or validation logic goes here
    # e.g., save to DynamoDB, S3, send notifications, etc.

    # Return success to Salesforce
    response_body = {
        "status": "ok",
        "message": "Cases received successfully",
        "workOrderId": work_order_id,
        "receivedCaseCount": len(cases),
    }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response_body)
    }
