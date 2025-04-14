import boto3
import json
import time
import re

textract = boto3.client('textract', region_name='eu-west-2')

def process(event, context):
    """
    Starts an asynchronous Textract document analysis job and processes the output.
    """
    bucket = event.get('bucket', 'metrosafetyprodfiles')
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
            
            # Optional: save processed data to S3 or another data store
            # store_output_to_s3(bucket, f"processed/{document_key.split('/')[-1].replace('.pdf', '.json')}", all_data)
            
            return {
                'statusCode': 200,
                'body': json.dumps(all_data)
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
    Combines pages into a full document text.
    """
    combined = ""
    for page in sorted(pages_text.keys()):
        combined += f"\n\n--- Page {page} ---\n\n"
        combined += pages_text[page]
    return combined.strip()

def extract_tables(blocks):
    """
    Extracts table data from Textract blocks.
    Returns a list of tables, each table is a list of rows and each row is a list of cell texts.
    """
    tables = []
    for block in blocks:
        if block.get("BlockType") == "TABLE":
            table = []
            # Collect all CELL blocks belonging to this table
            cell_blocks = []
            for rel in block.get("Relationships", []):
                if rel.get("Type") == "CHILD":
                    cell_blocks.extend([b for b in blocks if b["Id"] in rel.get("Ids", []) and b["BlockType"] == "CELL"])
            # Group cells by their RowIndex
            rows = {}
            for cell in cell_blocks:
                row_index = cell.get("RowIndex", 0)
                if row_index not in rows:
                    rows[row_index] = []
                cell_text = get_text_from_cell(cell, blocks)
                rows[row_index].append(cell_text)
            # Append rows sorted by row index to the table
            for row in sorted(rows.keys()):
                table.append(rows[row])
            tables.append(table)
    return tables

def get_text_from_cell(cell, blocks):
    """
    Extracts text contained in a table cell from its child WORD or LINE blocks.
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
    Extracts key-value pair data from Textract blocks.
    Returns a dictionary where keys are the field names and values are the corresponding values.
    """
    key_map = {}
    value_map = {}
    kvs = {}
    
    # Separate key and value blocks
    for block in blocks:
        if block.get("BlockType") == "KEY_VALUE_SET" and "EntityTypes" in block:
            if "KEY" in block.get("EntityTypes", []):
                key_map[block["Id"]] = block
            elif "VALUE" in block.get("EntityTypes", []):
                value_map[block["Id"]] = block

    # Link keys to values
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
    Helper function to extract text from a block by aggregating the text from its child WORD/LINE blocks.
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
    Processes the Textract JSON response to extract plain text, tables and key-value pairs.
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

# Optional: function to store output to S3 if required.
# def store_output_to_s3(bucket, key, data):
#     s3 = boto3.client('s3', region_name='eu-west-2')
#     s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))
    
if __name__ == "__main__":
    # For local testing only; simulate an event.
    test_event = {
        "bucket": "your-bucket-name",
        "document_key": "test.pdf",
        "sns_topic_arn": "arn:aws:sns:eu-west-2:123456789012:textract-job-notifications",
        "textract_role_arn": "arn:aws:iam::123456789012:role/TextractServiceRole"
    }
    result = process(test_event, None)
    print(json.dumps(result, indent=4))
