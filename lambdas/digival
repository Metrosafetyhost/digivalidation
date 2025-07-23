import json
import logging

# Initialise logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process(event, context):


    # Extract and parse the JSON body
    body_str = event.get('body') or ''
    try:
        payload = json.loads(body_str)
    except json.JSONDecodeError as e:
        logger.error(f'Invalid JSON received: {e}')
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Invalid JSON payload'})
        }

    # pull out the values
    work_order_id     = payload.get('workOrderId')
    work_order_number = payload.get('workOrderNumber')
    work_type_ref     = payload.get('workTypeRef')

    logger.info(
        'DigiValidation event received – workOrderId=%s, workOrderNumber=%s, workTypeRef=%s',
        work_order_id, work_order_number, work_type_ref
    )
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Event processed'})
    }