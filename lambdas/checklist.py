import boto3
import json
import time
import re
import csv
from io import StringIO

textract = boto3.client('textract', region_name='eu-west-2')

def process(event, context):
    """
    Starts an asynchronous Textract document analysis job and processes the output.
    """
    # Set default bucket name to "textrack output"
    bucket = event.get('bucket', 'textrack output')
    document_key = event.get('document_key', 'WorkOrders/your-document.pdf')
    
    sns_topic_arn = event.get('sns_topic_arn', 'arn:aws:sns:eu-west-2:837329614132:textract-job-notifications')
    textract_role_arn = event.get('textract_role_arn', 'arn:aws:iam::837329614132:role/TextractServiceRole')
    
    try:
        response = textract.start_document_analysis(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket,
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
        
        result = poll_for_job_completion(job_id)
        if result:
            all_data = process_all_data(result)
            print(json.dumps(all_data, indent=4))
            
            # Write the extracted data to a CSV file and store to S3.
            csv_content = generate_csv(all_data)
            csv_key = f"processed/{document_key.split('/')[-1].replace('.pdf', '.csv')}"
            store_output_to_s3(bucket, csv_key, csv_content)
            print(f"CSV output saved to s3://{bucket}/{csv_key}")
            
            return {
                'statusCode': 200,
                'body': json.dumps({"message": "Success", "csv_s3_key": csv_key})
            }
        else:
            raise Exception("Textract job did not complete successfully")
    
    except Exception as e:
        print("Error processing document:", e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def poll_for_job_completion(job_id, max_tries=20, delay=5):
    """
    Poll the Textract get_document_analysis endpoint until the job completes.
    """
    for _ in range(max_tries):
        response = textract.get_document_analysis(JobId=job_id)
        status = response.get('JobStatus')
        print(f"Job Status: {status}")
        if status == 'SUCCEEDED':
            return response
        elif status == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Textract job did not complete in the expected time.")

def process_textract_output(textract_response):
    """
    Process LINE blocks and groups them by page.
    Returns a dictionary with page numbers as keys and text as values.
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
    Combines pages into a single text string.
    """
    combined = ""
    for page in sorted(pages_text.keys()):
        combined += f"\n\n--- Page {page} ---\n\n"
        combined += pages_text[page]
    return combined.strip()

def extract_tables(blocks):
    """
    Extracts table data from Textract blocks.
    Returns a list of tables; each table is a list of rows and each row is a list of cell texts.
    """
    tables = []
    for block in blocks:
        if block.get("BlockType") == "TABLE":
            table = []
            # Collect all CELL blocks belonging to this table.
            cell_blocks = []
            for rel in block.get("Relationships", []):
                if rel.get("Type") == "CHILD":
                    cell_blocks.extend([b for b in blocks if b["Id"] in rel.get("Ids", []) and b["BlockType"] == "CELL"])
            # Group cells by their RowIndex.
            rows = {}
            for cell in cell_blocks:
                row_index = cell.get("RowIndex", 0)
                if row_index not in rows:
                    rows[row_index] = []
                cell_text = get_text_from_cell(cell, blocks)
                rows[row_index].append(cell_text)
            # Append rows sorted by row index to the table.
            for row in sorted(rows.keys()):
                table.append(rows[row])
            tables.append(table)
    return tables

def get_text_from_cell(cell, blocks):
    """
    Extracts text from a table cell from its child WORD or LINE blocks.
    """
    text = ""
    for rel in cell.get("Relationships", []):
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

def get_text_for_block(block, blocks):
    """
    Helper to aggregate text for a block from its child WORD/LINE blocks.
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

def process_all_data(textract_response):
    """
    Processes the Textract response and extracts pages of text, tables and key-value pairs.
    Returns a dictionary with keys: "text", "tables", and "form_data".
    """
    blocks = textract_response.get("Blocks", [])
    line_pages = process_textract_output(textract_response)
    combined_text = combine_pages(line_pages)
    tables = extract_tables(blocks)
    key_values = extract_key_values(blocks)
    
    output = {
        "text": combined_text,
        "tables": tables,
        "form_data": key_values
    }
    return output

def generate_csv(data):
    """
    Generates CSV content (as a string) from the extracted data.
    The CSV includes three sections: plain text, tables, and form_data.
    """
    output = StringIO()
    writer = csv.writer(output)
    
    # Section 1: Plain Text from pages
    writer.writerow(["=== Plain Text (Grouped by Page) ==="])
    # Split the combined text by pages (using the separator added in combine_pages)
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
    writer.writerow([])  # empty line
    
    # Section 2: Tables
    writer.writerow(["=== Tables ==="])
    for idx, table in enumerate(data["tables"], start=1):
        writer.writerow([f"Table {idx}"])
        for row in table:
            writer.writerow(row)
        writer.writerow([])  # blank row after each table

    # Section 3: Form Data (Key-Value Pairs)
    writer.writerow(["=== Form Data (Key-Value Pairs) ==="])
    writer.writerow(["Field", "Value"])
    for key, value in data["form_data"].items():
        writer.writerow([key, value])
    
    return output.getvalue()

def store_output_to_s3(bucket, key, content):
    """
    Stores the given CSV string to S3.
    """
    s3 = boto3.client('s3', region_name='eu-west-2')
    s3.put_object(Bucket=bucket, Key=key, Body=content, ContentType='text/csv')

if __name__ == "__main__":
    # For local testing; simulate an event.
    test_event = {
        "bucket": "textrack output",
        "document_key": "test.pdf",
        "sns_topic_arn": "arn:aws:sns:eu-west-2:123456789012:textract-job-notifications",
        "textract_role_arn": "arn:aws:iam::123456789012:role/TextractServiceRole"
    }
    result = process(test_event, None)
    print(json.dumps(result, indent=4))
