import csv
import json
from datetime import datetime, timezone
from pathlib import Path

WORK_ORDER_ID = "0WOSk000007nqizOAA"
WORK_ORDER_NUMBER = "00831747"

input_csv = Path("risk_assessment_questions.csv")
output_json = Path("risk_assessment_questions.json")

if not input_csv.exists():
    raise FileNotFoundError(f"Could not find {input_csv}")

with input_csv.open(newline="", encoding="utf-8-sig") as f:
    rows = list(csv.DictReader(f))

archive = {
    "archiveVersion": "1.0",
    "environment": "Sandbox",
    "salesforceObject": "Risk_Assessment_Question__c",
    "parentObject": "WorkOrder",
    "workOrderId": WORK_ORDER_ID,
    "workOrderNumber": WORK_ORDER_NUMBER,
    "exportedAt": datetime.now(timezone.utc).isoformat(),
    "recordCount": len(rows),
    "records": rows
}

with output_json.open("w", encoding="utf-8") as f:
    json.dump(archive, f, indent=2, ensure_ascii=False)

print(f"Wrote {len(rows)} records to {output_json}")