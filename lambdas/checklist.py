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
    processes the output, structures the text by sections, and stores the resulting CSV in a separate output bucket.
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
        
        # Poll for job completion and retrieve all pages.
        result = poll_for_job_completion(job_id)
        
        # Log the raw Textract JSON response (all pages)
        print("Raw Textract JSON response (all pages):")
        print(json.dumps(result, indent=4, default=str))
        
        # Process the Textract output into combined plain text.
        all_data = process_all_data(result)
        print("Processed Textract plain text:")
        print(all_data["text"])
        
        # STRUCTURE THE DOCUMENT
        structured_doc = structure_document(all_data["text"])
        print("Structured Document (by section):")
        print(json.dumps(structured_doc, indent=4, default=str))
            
        # Generate a CSV from the structured text.
        csv_content = generate_structured_csv(structured_doc)
        csv_key = f"processed/{document_key.split('/')[-1].replace('.pdf', '_structured.csv')}"
        store_output_to_s3(output_bucket, csv_key, csv_content)
        print(f"Structured CSV output saved to s3://{output_bucket}/{csv_key}")
            
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
    Once complete, retrieve all pages.
    """
    for _ in range(max_tries):
        response = textract.get_document_analysis(JobId=job_id)
        status = response.get('JobStatus')
        print(f"Job Status: {status}")
        if status == 'SUCCEEDED':
            return get_all_pages(job_id)
        elif status == 'FAILED':
            raise Exception("Textract job failed")
        time.sleep(delay)
    raise Exception("Textract job did not complete in the expected time.")

def get_all_pages(job_id):
    """
    Retrieve all pages from Textract after the job has completed.
    Accumulates all Blocks from all pages using NextToken.
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

def extract_tables(blocks):
    """
    Extracts table data from Textract blocks.
    Returns a list of tables; each table is a list of rows (each row is a list of cell texts).
    Includes debug logs to show what is being processed.
    """
    tables = []
    for block in blocks:
        if block.get("BlockType") == "TABLE":
            print(f"Found TABLE block with Id: {block.get('Id')}, Relationships: {block.get('Relationships')}")
            table = []
            cell_blocks = []
            for rel in block.get("Relationships", []):
                if rel.get("Type") == "CHILD":
                    cell_blocks.extend([b for b in blocks if b["Id"] in rel.get("Ids", []) and b["BlockType"] == "CELL"])
            print(f"Extracted {len(cell_blocks)} CELL blocks for TABLE Id: {block.get('Id')}")
            rows = {}
            for cell in cell_blocks:
                row_index = cell.get("RowIndex", 0)
                if row_index not in rows:
                    rows[row_index] = []
                cell_text = get_text_from_cell(cell, blocks)
                rows[row_index].append(cell_text)
            for row in sorted(rows.keys()):
                table.append(rows[row])
            tables.append(table)
    return tables

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

def process_all_data(textract_response):
    """
    Processes the Textract response to extract plain text, tables, and key-value pairs.
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

def structure_document(text):
    """
    Splits the combined text into sections based on known headings.  
    Adjust the list of headings as needed for your PDF's layout.
    Returns a dictionary with each section heading as key and the corresponding text as value.
    """
    # Define a list of common headings (use exact text if possible or a regex pattern)
    headings = [
        "1.0 Executive Summary",
        "1.1 Areas Identified Requiring Remedial Actions",
        "1.2 Building Description",
        "1.3 Water Scope",
        "2.0 Risk Dashboard",
        "2.1 Current Risk Ratings",
        "3.0 Management Responsibilities",
        "4.0 Legionella Control Programme of Preventative Works",
        "5.0 Audit Detail",
        "6.0 Additional Photos",
        "7.0 System Asset Register",
        "8.0 Water Assets",
        "9.0 Appendices"
    ]
    
    structured = {}
    # Start with a "Preface" section for any text before the first known heading.
    current_heading = "Preface"
    structured[current_heading] = []
    
    # Split text into lines.
    lines = text.splitlines()
    for line in lines:
        # Check if this line contains one of the headings.
        found = False
        for heading in headings:
            # Using a simple 'in' check; you can enhance this with regex if needed.
            if heading.lower() in line.lower():
                current_heading = heading
                if current_heading not in structured:
                    structured[current_heading] = []
                found = True
                break
        if not found:
            structured[current_heading].append(line)
    
    # Join lines back together for each section.
    for heading in structured:
        structured[heading] = "\n".join(structured[heading]).strip()
    return structured

def generate_structured_csv(structured_doc):
    """
    Generates a CSV string from the structured document.
    Each row represents a section with two columns: Section and Content.
    """
    output = StringIO()
    writer = csv.writer(output)
    
    # Write header row.
    writer.writerow(["Section", "Content"])
    
    for section, content in structured_doc.items():
        writer.writerow([section, content])
    
    return output.getvalue()

def generate_csv(data):
    """
    (Existing function to generate a CSV from unstructured data.)
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
    for idx, table in enumerate(data["tables"], start=1):
        writer.writerow([f"Table {idx}"])
        for row in table:
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
        "document_key": "WorkOrders/0WOSk0000036JRFOA2/065339-15-02-2025-b-and-m-st-nicholas-dr_tbp_v1_final.pdf",
        "sns_topic_arn": "arn:aws:sns:eu-west-2:123456789012:textract-job-notifications",
        "textract_role_arn": "arn:aws:iam::123456789012:role/TextractServiceRole"
    }
    result = process(test_event, None)
    print(json.dumps(result, indent=4, default=str))
