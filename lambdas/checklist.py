import boto3
import json
import os
import fitz  # PyMuPDF

# Create S3 client and Textract client
s3 = boto3.client('s3')
textract = boto3.client('textract', region_name='eu-west-2')

def flatten_pdf(input_path, output_path):
    """Opens a PDF and saves a flattened copy."""
    doc = fitz.open(input_path)
    # The 'clean' and 'deflate' options generally help in removing form fields and annotations.
    doc.save(output_path, garbage=4, deflate=True, clean=True)
    doc.close()

def process(event, context):
    bucket = event.get('bucket', 'metrosafetyprodfiles')
    document_key = event.get('document_key', 'WorkOrders/0WOSk0000036JRFOA2/065339-15-02-2025-b-and-m-st-nicholas-hs-unit-b-st-nicholas-dr_tbp_v1_final.pdf')

    try:
        # Download the file from S3 to /tmp directory (Lambda's temporary storage)
        local_input = f"/tmp/input.pdf"
        local_flattened = f"/tmp/flattened.pdf"
        s3.download_file(bucket, document_key, local_input)

        # Flatten the PDF
        flatten_pdf(local_input, local_flattened)

        # Now call Textract on the flattened PDF by uploading it back to S3 or reading bytes.
        # Option A: Upload the flattened PDF back to S3 (if you want to reuse it later)
        flattened_key = "flattened/" + os.path.basename(document_key)
        s3.upload_file(local_flattened, bucket, flattened_key)

        # Use Textract analyze_document on the S3 object (flattened PDF)
        response = textract.analyze_document(
            Document={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': flattened_key
                }
            },
            FeatureTypes=['TABLES', 'FORMS']
        )

        # Process the Textract response as needed (this part of your code remains the same)
        pages_text = process_textract_output(response)
        combined_text = combine_pages(pages_text)
        sections = parse_sections(combined_text)

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
    # Local test event
    test_event = {
        "bucket": "metrosafetyprodfiles",
        "document_key": "WorkOrders/0WOSk0000036JRFOA2/065339-15-02-2025-b-and-m-st-nicholas-hs-unit-b-st-nicholas-dr_tbp_v1_final.pdf"
    }
    result = process(test_event, None)
    print(json.dumps(result, indent=4))