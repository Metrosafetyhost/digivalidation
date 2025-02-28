import json
from bs4 import BeautifulSoup
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger  = Logger()

# define the list of headers you want to proof, could consider other ways of doing this, 
# as this could be tedious finding all possible headers. 
ALLOWED_HEADERS = [
    "Passenger and Disabled Access Platform Lifts (DAPL)",
    "Fire Service and Evacuation Lifts",
    "Mains Electrical incomers and electrical distribution boards (EDBs)",
    "Natural Gas Supplies",
    "Fire Safety",
    "Roof Details"
]
TESTING_LOCALLY = True

def call_bedrock(text):
    
    print(f"Checking Text: {text}")  # print the extracted text before proofing

    # proofing process: some examples errors i found within the document.  
    proofed_text = text.replace("exemple", "example").replace("Ths", "This")

    print(f" Proofed Text: {proofed_text}\n")

    return proofed_text

@logger.inject_lambda_context()
def process(event: dict, context: LambdaContext)-> str:
    body = json.loads(event.get("body", "{}"))
    html_data_list = body.get("htmlData", [])  # List of HTML tables

    proofed_html_list = []

    # process each HTML table in the json
    for html_table in html_data_list:
        soup = BeautifulSoup(html_table, 'html.parser')
        rows = soup.find_all('tr')

        proofing_requests = {}  # store text needing proofing
        proofed_texts = {}  # store roofed results

        # extract key-value pairs from table rows
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 2:
                header = cells[0].get_text(strip=True)
                content = cells[1].get_text(strip=True)

                if header in ALLOWED_HEADERS:
                    print(f"Extracting header: {header}") 
                    proofing_requests[header] = content

        # send selected content to AWS
        for header, text in proofing_requests.items():
            proofed_texts[header] = call_bedrock(text)

        # rebuild table with proofed text
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 2:
                header = cells[0].get_text(strip=True)
                
                # replace only if the header matches proofed results
                if header in proofed_texts:
                    new_content = proofed_texts[header]
                    cells[1].string = new_content  # Replace text while keeping HTML structure

        # store updated HTML table
        proofed_html_list.append(str(soup))
    
    print("✅ Finished proofing. Returning proofed HTML.\n")
 
    # return the proofed HTML data as a list (matching the input format)
    return {
        "statusCode": 200,
        "body": json.dumps({"proofed_html": proofed_html_list})
    }
   

    
if __name__ == "__main__":
    print("Running handlersalesforvr.py...")
    
    # load my test json file
  

    test_event = {"body": json.dumps(json_data)}

    response = lambda_handler(test_event, None)

    # print nicely
    print(json.dumps(json.loads(response["body"]), indent=4))
