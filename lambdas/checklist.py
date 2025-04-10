import boto3
import json
import re

# Initialise the Textract client
textract = boto3.client('textract')

def process(event, context):
    """
    Test event should include:
    {
        "bucket": "your-bucket-name",
        "document_key": "folder1/folder2/test.pdf"
    }
    """
    bucket = event.get('bucket', 'metrosafetyprodfiles')
    document_key = event.get('document_key', 'WorkOrders/0WOSk0000036JRFOA2/065339~15-02-2025# b&m st nicholas hs unit b  st nicholas dr_tbp_v1_final.p.pdf')
    
    try:
        # Call Textract to analyze the document; using TABLES and FORMS as extra features (adjust if needed)
        response = textract.analyze_document(
            Document={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': document_key
                }
            },
            FeatureTypes=['TABLES', 'FORMS']
        )
        
        # First, process the raw Textract output to group text lines per page
        pages_text = process_textract_output(response)
        
        # Combine pages (ordered by page number) into one large text
        combined_text = combine_pages(pages_text)
        
        # Now parse the combined text into sections based on detected headings
        sections = parse_sections(combined_text)
        
        # For testing, print the grouped sections in JSON format
        print(json.dumps(sections, indent=4))
        
        return {
            'statusCode': 200,
            'body': json.dumps(sections)
        }
    except Exception as e:
        print("Error processing document:", e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def process_textract_output(textract_response):
    """
    Processes the Textract JSON response and groups 'LINE' blocks by page.
    Returns a dictionary of page numbers and their corresponding text.
    """
    pages = {}
    for block in textract_response.get('Blocks', []):
        if block.get('BlockType') == 'LINE':
            # Some multi-page documents have a "Page" attribute; default to 1 if not present
            page_number = block.get('Page', 1)
            if page_number not in pages:
                pages[page_number] = []
            top_val = block.get('Geometry', {}).get('BoundingBox', {}).get('Top', 0)
            pages[page_number].append((top_val, block.get('Text', '')))
    
    # Sort and concatenate the lines for each page
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
        # Optionally, you can add page breaks for clarity.
        combined += f"\n\n--- Page {page} ---\n\n"
        combined += pages_text[page]
    return combined

def parse_sections(text):
    """
    Parse the combined text into sections using regular expression matching.
    
    We assume section headings start with patterns like:
      - "1.0 Executive Summary"
      - "1.1 Areas Identified Requiring Remedial Actions"
      - "2.0 Risk Dashboard" etc.
    Also, headings for appendices like "APPENDIX A - Duty Holder's Responsibilities" are captured.
    
    Returns a dictionary where keys are section headings and values are the body text of that section.
    """
    # Regex pattern to capture typical section headers.
    # This pattern matches lines that start with digits and a dot OR lines that start with "APPENDIX".
    pattern = re.compile(r'^(?P<section>(?:\d+\.\d+(?:\.\d+)?\s+.*|APPENDIX\s+[A-Z]+\s*-\s*.*))$', re.MULTILINE)
    
    matches = list(pattern.finditer(text))
    sections = {}
    
    # If no section headings are found, return the whole text as one section.
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
        "document_key": "test.pdf"  # Adjust the path as needed (e.g., "folder1/test.pdf")
    }
    # For local testing, context can be None.
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=4))
