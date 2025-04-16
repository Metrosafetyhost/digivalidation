import boto3
import json
import re

# Initialise the Bedrock and S3 clients – adjust region as required.
bedrock = boto3.client('bedrock-runtime', region_name='eu-west-2')
s3 = boto3.client('s3', region_name='eu-west-2')

def extract_section(text, section_title):
    """
    Extracts content from the CSV text between a section header and the next header.
    Adjust this if your CSV formatting is different.
    """
    pattern = re.compile(re.escape(section_title) + r':\s*(.*?)\s*(?=\w+:\s|$)', re.DOTALL | re.IGNORECASE)
    match = pattern.search(text)
    return match.group(1).strip() if match else ""

def extract_csv_data(csv_content):
    """
    Extracts required sections from the CSV content.
    You can add additional sections here as needed.
    """
    water_description = extract_section(csv_content, "Water Description")
    water_assets = extract_section(csv_content, "Water Assets")
    # Return a dictionary so you can easily extend to include more sections.
    return {
        "water_description": water_description,
        "water_assets": water_assets
    }

def build_prompt(question_number, extracted_data):
    """
    Builds the prompt according to the specific QCC question.
    For now, this function handles question 5; you can expand it for all 23.
    """
    if question_number == 5:
        prompt = (
            "Water Hygiene/Legionella Risk Assessment QCC Query:\n\n"
            "Question 5: Read the Water Description and then cross-check it with the Water Assets "
            "in the back of the report. For example, ensure that if the Water Description states '1 x mains cold water, "
            "1 x CWS Cistern and 2 x HW Calorifiers', then the Water Assets section reflects this exactly.\n\n"
            f"--- Water Description ---\n{extracted_data.get('water_description')}\n\n"
            f"--- Water Assets ---\n{extracted_data.get('water_assets')}\n\n"
            "Please provide a summary of any discrepancies or confirm that the information is consistent."
        )
        return prompt
    else:
        # Future QCC questions can be added here.
        return "No valid question number provided."

def send_question_to_bedrock(prompt):
    """
    Sends the custom prompt to Bedrock and returns the response.
    Adjust ModelId and parameters as per your Bedrock configuration.
    """
    response = bedrock.invoke_model(
        ModelId='your-bedrock-model-id',  # Replace with your actual Bedrock model ID.
        Body=json.dumps({
            'prompt': prompt,
            'maxTokens': 500,
            # You can include additional parameters as needed.
        }),
        ContentType='application/json'
    )
    result = json.loads(response['Body'].read().decode('utf-8'))
    return result

def process(event, context):
    """
    Main Lambda handler.
    
    Expected event parameters:
      - csv_bucket: The S3 bucket name where the CSV is stored.
      - csv_key: The key (path) for the CSV file.
      - question_number: The QCC question number to be processed (e.g. 5).
    """
    csv_bucket = event.get('csv_bucket', 'your-csv-bucket')
    csv_key = event.get('csv_key', 'processed/your_extracted_file.csv')
    question_number = event.get('question_number', 5)  # Default to question 5 for now
    
    # Retrieve the CSV content from S3.
    try:
        csv_obj = s3.get_object(Bucket=csv_bucket, Key=csv_key)
        csv_content = csv_obj['Body'].read().decode('utf-8')
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Error reading CSV from S3: {str(e)}"})
        }
    
    # Extract the required sections from the CSV.
    extracted_data = extract_csv_data(csv_content)
    
    # Build the prompt using the specific QCC question.
    prompt = build_prompt(question_number, extracted_data)
    
    # Invoke Bedrock with the constructed prompt.
    try:
        bedrock_response = send_question_to_bedrock(prompt)
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Error invoking Bedrock: {str(e)}"})
        }
    
    # Return the Bedrock response.
    return {
        'statusCode': 200,
        'body': json.dumps({'bedrock_response': bedrock_response})
    }

if __name__ == '__main__':
    # Local testing event; update parameters as necessary.
    test_event = {
        "csv_bucket": "your-csv-bucket",
        "csv_key": "processed/your_extracted_file.csv",
        "question_number": 5  # Currently testing for question 5.
    }
    result = process(test_event, None)
    print(json.dumps(result, indent=4))
