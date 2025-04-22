import json
import logging
from datetime import datetime
from typing import Dict, Any
from layers.utils import log_to_sns, get_secrets
import boto3
import os

# Initialize AWS services
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Load secrets
secrets = get_secrets()

# SNS TOPIC ARNS
EXPORT_ORDER_HISTORY_TOPIC_ARN = secrets["EXPORT_ORDER_HISTORY_TOPIC_ARN"]

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def validate_date_range(start_date: str, end_date: str) -> bool:
    """Validate the date range for the export request."""
    try:
        start = datetime.fromisoformat(start_date)
        end = datetime.fromisoformat(end_date)
        
        # Check if dates are valid
        if start > end:
            return False
            
        # Check if date range is not more than 1 year
        if (end - start).days > 365:
            return False
            
        return True
    except ValueError:
        return False

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function to handle order history export requests.
    Validates the request and publishes to SNS for async processing.
    
    Expected body parameters:
    - userid: user's ID
    - format: 'csv', 'pdf', or 'excel'
    - start_date: ISO-8601 date string
    - end_date: ISO-8601 date string
    - service_type: 'car_wash' or 'trashbin' (optional)
    """
    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        user_id = body.get('userid')
        export_format = body.get('format')
        start_date = body.get('start_date')
        end_date = body.get('end_date')
        service_type = body.get('service_type')
        
        # Validate required parameters
        if not all([user_id, export_format, start_date, end_date]):
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required parameters: userid, format, start_date, end_date'
                })
            }
        
        # Validate export format
        valid_formats = ['csv', 'pdf', 'excel']
        if export_format.lower() not in valid_formats:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Invalid format. Must be one of: {", ".join(valid_formats)}'
                })
            }
        
        # Validate date range
        if not validate_date_range(start_date, end_date):
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Invalid date range. Dates must be valid and range cannot exceed 1 year.'
                })
            }
        
        # Validate service type if provided
        if service_type:
            valid_service_types = ['car_wash', 'trashbin']
            if service_type not in valid_service_types:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': f'Invalid service_type. Must be one of: {", ".join(valid_service_types)}'
                    })
                }
        
        # Prepare export request payload
        export_request = {
            'userid': user_id,
            'format': export_format.lower(),
            'start_date': start_date,
            'end_date': end_date,
            'service_type': service_type,
            'request_id': f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{user_id}",
            'requested_at': datetime.now().isoformat()
        }
        
        sns_client.publish(
            TopicArn=EXPORT_ORDER_HISTORY_TOPIC_ARN,
            Message=json.dumps(export_request),
            Subject=f"Order History Export Request - {export_request['request_id']}"
        )

        logger.info(f"Export request published to SNS: {export_request['request_id']}")
        
        return {
            'statusCode': 202, 
            'body': json.dumps({
                'message': 'Export request accepted',
                'request_id': export_request['request_id'],
                'status': 'processing'
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing export request: {str(e)}", exc_info=True)
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
