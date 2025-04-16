import boto3
import json
import time
import re
import csv
from io import StringIO

textract = boto3.client('textract', region_name='eu-west-2')

def process(event, context):
    """
    Starts an asynchronous Textract document analysis job on a PDF stored in the input bucket,
    processes the output, and stores the resulting CSV in a separate output bucket.
    """
    # Retrieve input parameters:
    input_bucket = event.get('bucket', 'metrosafetyprodfiles')
    document_key = event.get('document_key', 'WorkOrders/your-document.pdf')
    output_bucket = event.get('output_bucket', 'textract-output-digival')
    
    sns_topic_arn = event.get('sns_topic_arn', 'arn:aws:sns:eu-west-2:837329614132:textract-job-notifications')
    textract_role_arn = event.get('textract_role_arn', 'arn:aws:iam::837329614132:role/TextractServiceRole')
    
    try:
        # Start the asynchronous Textract analysis on the input document.
        response = textract.start_document_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': input_bucket,
                    'Name': document_key
                }
            },
            FeatureTypes=['TABLES', 'FORMS'],
            NotificationChannel={
                'SNSTopicArn': sns_topic_arn,
                'RoleArn': textract_role_arn
            }
        )
        
        job_id = response['JobId']
        print(f"Started Textract job with ID: {job_id}")
        
        # Poll for job completion; this returns a combined response across all pages.
        result = poll_for_job_completion(job_id)
        
        # Log the full Textract JSON (all accumulated blocks) so you can inspect it.
        print("Raw Textract JSON response (all pages):")
        print(json.dumps(result, indent=4, default=str))
        
        # Process the Textract output into a more manageable structure.
        all_data = process_all_data(result)
        print("Processed Textract data:")
        print(json.dumps(all_data, indent=4, default=str))
            
        # Generate CSV content and store it in the output bucket.
        csv_content = generate_csv(all_data)
        csv_key = f"processed/{document_key.split('/')[-1].replace('.pdf', '.csv')}"
        store_output_to_s3(output_bucket, csv_key, csv_content)
        print(f"CSV output saved to s3://{output_bucket}/{csv_key}")
            
        return {
            'statusCode': 200,
            'body': json.dumps({"message": "Success", "csv_s3_key": csv_key})
        }
    
    except Exception as e:
        print("Error processing document:", e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def poll_for_job_completion(job_id, max_tries=20, delay=5):
    """
    Poll the Textract get_document_analysis endpoint until the job completes.
    Once the job status is SUCCEEDED, use the helper get_all_pages() to retrieve all pages.
    """
    for _ in range(max_tries):
        response = textract.get_document_analysis(JobId=job_id)
        status = response.get('JobStatus')
        print(f"Job Status: {status}")
        if status == 'SUCCEEDED':
            # The job is complete, now retrieve all pages.
            return get_all_pages(job_id)
        elif status == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Textract job did not complete in the expected time.")

def get_all_pages(job_id):
    """
    Retrieve all pages from Textract after the job has completed.
    This function accumulates the 'Blocks' from all pages using NextToken.
    """
    all_blocks = []
    next_token = None
    while True:
        if next_token:
            response = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            response = textract.get_document_analysis(JobId=job_id)
        print(f"Fetching page with NextToken: {next_token}")
        all_blocks.extend(response.get("Blocks", []))
        next_token = response.get("NextToken")
        if not next_token:
            break
    return {"Blocks": all_blocks}

def process_textract_output(textract_response):
    """
    Processes LINE blocks and groups them by page.
    Returns a dictionary with page numbers as keys and their aggregated text as values.
    """
    pages = {}
    for block in textract_response.get('Blocks', []):
        if block.get('BlockType') == 'LINE':
            page_number = block.get('Page', 1)
            if page_number not in pages:
                pages[page_number] = []
            top_val = block.get('Geometry', {}).get('BoundingBox', {}).get('Top', 0)
            pages[page_number].append((top_val, block.get('Text', '')))
    
    structured_pages = {}
    for page_num, lines in pages.items():
        sorted_lines = sorted(lines, key=lambda x: x[0])
        page_text = "\n".join(line[1] for line in sorted_lines)
        structured_pages[page_num] = page_text
    return structured_pages

def combine_pages(pages_text):
    """
    Combines pages into a single string with page separators.
    """
    combined = ""
    for page in sorted(pages_text.keys()):
        combined += f"\n\n--- Page {page} ---\n\n"
        combined += pages_text[page]
    return combined.strip()

def get_text_from_cell(cell, blocks):
    """
    Extracts the text from a table cell using its child WORD or LINE blocks.
    """
    text = ""
    for rel in cell.get("Relationships", []):
        if rel.get("Type") == "CHILD":
            for child_id in rel.get("Ids", []):
                child = next((b for b in blocks if b["Id"] == child_id), None)
                if child and child.get("BlockType") in ["WORD", "LINE"]:
                    text += child.get("Text", "") + " "
    return text.strip()

def get_text_for_block(block, blocks):
    """
    Helper function to extract aggregated text for a block from its child WORD/LINE blocks.
    """
    text = ""
    if "Relationships" in block:
        for rel in block["Relationships"]:
            if rel.get("Type") == "CHILD":
                for child_id in rel.get("Ids", []):
                    child = next((b for b in blocks if b["Id"] == child_id), None)
                    if child and child.get("BlockType") in ["WORD", "LINE"]:
                        text += child.get("Text", "") + " "
    return text.strip()

def extract_key_values(blocks):
    """
    Extracts key-value data from Textract blocks.
    Returns a dictionary mapping keys to their corresponding values.
    """
    key_map = {}
    value_map = {}
    kvs = {}
    
    for block in blocks:
        if block.get("BlockType") == "KEY_VALUE_SET" and "EntityTypes" in block:
            if "KEY" in block.get("EntityTypes", []):
                key_map[block["Id"]] = block
            elif "VALUE" in block.get("EntityTypes", []):
                value_map[block["Id"]] = block

    for key_id, key_block in key_map.items():
        key_text = get_text_for_block(key_block, blocks)
        associated_value_text = ""
        if "Relationships" in key_block:
            for rel in key_block["Relationships"]:
                if rel.get("Type") == "VALUE":
                    for value_id in rel.get("Ids", []):
                        value_block = value_map.get(value_id)
                        if value_block:
                            associated_value_text += get_text_for_block(value_block, blocks) + " "
        kvs[key_text] = associated_value_text.strip()
    return kvs

def extract_tables_grouped(blocks):
    """
    Extracts table data from Textract blocks, grouping tables under the last seen major heading per page.
    Returns a list of dictionaries, each containing:
        - 'rows': the table data (list of rows),
        - 'header': the inherited heading text for that table (or None),
        - 'page': the page number.
    """
    important_headings = [
        "Significant Findings and Action Plan",
        "Executive Summary",
        "Areas Identified Requiring Remedial Actions",
        "Building Description",
        "Water Scope",
        "Risk Dashboard",
        "Management Responsibilities",
        "Legionella Control Programme",
        "Audit Detail",
        "System Asset Register",
        "Water Assets",
        "Appendices",
        "Risk Assessment Checklist",
        "Legionella Control Programme of Preventative Works",
    ]

    def is_major_heading(text):
        txt = text.lower().strip()
        return any(phrase.lower() in txt for phrase in important_headings)

    # Pre‑collect all the LINE blocks so we can do a spatial lookup
    line_blocks = [
        b for b in blocks
        if b.get("BlockType") == "LINE" and "Geometry" in b
    ]

    def find_heading_for_table(tbl):
        # Find the closest LINE block whose bottom edge sits just above this table's top.
        top = tbl["Geometry"]["BoundingBox"]["Top"]
        candidates = []
        for ln in line_blocks:
            bbox = ln["Geometry"]["BoundingBox"]
            bottom = bbox["Top"] + bbox["Height"]
            gap = top - bottom
            # only consider lines that are above the table (gap > 0), within a small threshold
            if 0 < gap < 0.03:
                candidates.append((gap, ln["Text"].strip()))
        if not candidates:
            return None
        # smallest vertical gap → the heading
        return min(candidates, key=lambda x: x[0])[1]

    # Build a page map of LINE and TABLE blocks.
    page_map = {}
    for b in blocks:
        page = b.get("Page", 1)
        if b.get("BlockType") in ("LINE", "TABLE"):
            page_map.setdefault(page, []).append(b)

    output_tables = []
    # iterate page by page
    for page, items in page_map.items():
        # sort by vertical position
        items.sort(key=lambda b: b["Geometry"]["BoundingBox"]["Top"])
        current_heading = None

        for b in items:
            if b["BlockType"] == "LINE":
                txt = b.get("Text", "").strip()
                if is_major_heading(txt):
                    current_heading = txt
            else:  # TABLE
                # First try to grab a hard‐matched heading,
                # else fall back to spatially finding the nearest line above the table.
                header = current_heading or find_heading_for_table(b)

                # now extract the table cells as before
                table_dict = {"page": page, "header": header, "rows": []}
                cell_blocks = []
                for rel in b.get("Relationships", []):
                    if rel.get("Type") == "CHILD":
                        cell_blocks.extend(
                            [blk for blk in blocks
                             if blk["Id"] in rel["Ids"] and blk["BlockType"] == "CELL"]
                        )

                rows = {}
                for cell in cell_blocks:
                    ridx = cell.get("RowIndex", 0)
                    rows.setdefault(ridx, []).append(get_text_from_cell(cell, blocks))
                for ridx in sorted(rows):
                    table_dict["rows"].append(rows[ridx])

                output_tables.append(table_dict)

    return output_tables

def process_all_data(textract_response):
    """
    Processes the Textract response to extract plain text, tables, and key-value pairs.
    Returns a dictionary with keys: "text", "tables", and "form_data".
    """
    blocks = textract_response.get("Blocks", [])
    line_pages = process_textract_output(textract_response)
    combined_text = combine_pages(line_pages)
    # Use the new grouped table extraction function.
    tables = extract_tables_grouped(blocks)
    key_values = extract_key_values(blocks)
    
    output = {
        "text": combined_text,
        "tables": tables,
        "form_data": key_values
    }
    return output

def generate_csv(data):
    """
    Generates a CSV string from the extracted data with three sections:
    plain text (grouped by page), tables, and form data.
    For tables, if a header was extracted dynamically, it will be used in place of a generic "Table {number}".
    """
    output = StringIO()
    writer = csv.writer(output)
    
    # Section 1: Plain Text from pages.
    writer.writerow(["=== Plain Text (Grouped by Page) ==="])
    pages = data["text"].split("\n\n--- Page ")
    for page in pages:
        if page.strip():
            header_split = page.split("---", 1)
            if len(header_split) == 2:
                page_header = header_split[0].strip()
                page_text = header_split[1].strip()
            else:
                page_header = ""
                page_text = header_split[0].strip()
            writer.writerow([f"Page {page_header}", page_text])
    writer.writerow([])  # Empty line
    
    # Section 2: Tables.
    writer.writerow(["=== Tables ==="])
    for idx, table_info in enumerate(data["tables"], start=1):
        header_text = table_info.get("header") or f"Table {idx}"
        writer.writerow([header_text])
        for row in table_info.get("rows", []):
            writer.writerow(row)
        writer.writerow([])  # Blank row after each table

    # Section 3: Form Data (Key-Value Pairs).
    writer.writerow(["=== Form Data (Key-Value Pairs) ==="])
    writer.writerow(["Field", "Value"])
    for key, value in data["form_data"].items():
        writer.writerow([key, value])
    
    return output.getvalue()

def store_output_to_s3(bucket, key, content):
    """
    Stores the given CSV string (content) to the specified S3 bucket.
    """
    s3 = boto3.client('s3', region_name='eu-west-2')
    s3.put_object(Bucket=bucket, Key=key, Body=content, ContentType='text/csv')

if __name__ == "__main__":
    # For local testing; simulate an event.
    test_event = {
        "bucket": "metrosafetyprodfiles",  # Input bucket for the PDF
        "document_key": "WorkOrders/0WOSk0000036JRFOA2/065339-15-02-2025-b-and-m-st-nicholas-hs-unit-b-st-nicholas-dr_tbp_v1_final.pdf",
        "sns_topic_arn": "arn:aws:sns:eu-west-2:123456789012:textract-job-notifications",
        "textract_role_arn": "arn:aws:iam::123456789012:role/TextractServiceRole",
        # Optionally, you could pass "output_bucket": "desired-output-bucket" if you want a different name.
    }
    result = process(test_event, None)
    print(json.dumps(result, indent=4, default=str))