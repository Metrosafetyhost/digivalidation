import boto3
import json
import time

textract = boto3.client('textract', region_name='eu-west-2')

def process(event, context):
    """
    Starts an asynchronous Textract document analysis job.
    """
    bucket = event.get('bucket', 'metrosafetyprodfiles')
    document_key = event.get('document_key', 'WorkOrders/your-document.pdf')
    
    # Replace these with your actual SNS Topic ARN and the IAM Role ARN (the one created for Textract)
    sns_topic_arn = event.get('sns_topic_arn', 'arn:aws:sns:eu-west-2:837329614132:textract-job-notifications')
    textract_role_arn = event.get('textract_role_arn', 'arn:aws:iam::837329614132:role/bedrock-lambda-checklist')
    
    try:
        # Start the asynchronous document analysis job with notification channel details.
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
        
        # Optional: poll for job completion (or use the SNS notification to trigger your next step)
        result = poll_for_job_completion(job_id)
        if result:
            pages_text = process_textract_output(result)
            combined_text = combine_pages(pages_text)
            sections = parse_sections(combined_text)
            
            print(json.dumps(sections, indent=4))
            
            return {
                'statusCode': 200,
                'body': json.dumps(sections)
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
    Poll the Textract get_document_analysis endpoint until job completes (or max_tries are reached).
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
    Processes the Textract JSON response and groups 'LINE' blocks by page.
    Returns a dictionary of page numbers and their corresponding text.
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
    Combine the pages (sorted by page number) into one overall document text.
    """
    combined = ""
    for page in sorted(pages_text.keys()):
        combined += f"\n\n--- Page {page} ---\n\n"
        combined += pages_text[page]
    return combined

def parse_sections(text):
    """
    Parse the combined text into sections using regex for section headings.
    """
    import re
    pattern = re.compile(r'^(?P<section>(?:\d+\.\d+(?:\.\d+)?\s+.*|APPENDIX\s+[A-Z]+\s*-\s*.*))$', re.MULTILINE)
    matches = list(pattern.finditer(text))
    sections = {}
    
    if not matches:
        sections["Entire Document"] = text.strip()
        return sections
    
    for i, match in enumerate(matches):
        section_heading = match.group('section').strip()
        start_index = match.end()
        if i < len(matches) - 1:
            end_index = matches[i+1].start()
        else:
            end_index = len(text)
        section_body = text[start_index:end_index].strip()
        sections[section_heading] = section_body
    return sections

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
