import json
import boto3
import logging
from datetime import datetime

from layers.utils import get_secrets, log_to_sns

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
NOTIFICATIONS_TOPIC_ARN = secrets["NOTIFICATIONS_TOPIC_ARN"]


def lambda_handler(event, context):
    """
    Lambda handler that publishes notification messages to SNS.

    Example usage:
    {
        "user_id": "123",
        "notification_type": "ORDER_STATUS_CHANGE",
        "notification_data": {
            "order_id": "ABC123",
            "status": "Delivered"
        },
        "language": "en"
    }
    """
    try:
        # Parse the request body
        body = json.loads(event.get('body', '{}'))

        # Extract parameters
        user_id = body.get('user_id')
        notification_type = body.get('notification_type')
        notification_data = body.get('notification_data', {})
        language = body.get('language', 'en')

        # Validate required parameters
        if not user_id or not notification_type:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: user_id and notification_type are required'
                })
            }

        # Validate notification type
        valid_notification_types = [
            "ORDER_STATUS_CHANGE", "NEW_MESSAGE", "PAYMENT_CONFIRMATION",
            "PROVIDER_ASSIGNED", "SERVICE_REMINDER", "RATING_REMINDER",
            "DOCUMENT_READY", "COMPLAINT_UPDATE"
        ]

        if notification_type not in valid_notification_types:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': f'Invalid notification_type. Must be one of: {", ".join(valid_notification_types)}'
                })
            }

        # Prepare notification message
        notification_message = {
            'user_id': user_id,
            'notification_type': notification_type,
            'notification_data': notification_data,
            'language': language,
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS notifications topic
        response = sns_client.publish(
            TopicArn=NOTIFICATIONS_TOPIC_ARN,
            Message=json.dumps(notification_message),
            Subject=f'Notification: {notification_type}'
        )

        message_id = response.get('MessageId')

        # Log to SNS
        log_to_sns(1, 7, 15, 1, {
            "user_id": user_id,
            "notification_type": notification_type,
            "message_id": message_id
        }, "Send Notification Request - Success", user_id)

        return {
            'statusCode': 202,  # Accepted
            'body': json.dumps({
                'message': 'Notification request accepted for processing',
                'message_id': message_id
            })
        }

    except Exception as e:
        logger.error(f"Error sending notification request: {str(e)}")

        # Log error to SNS
        log_to_sns(4, 7, 15, 43, {
            "error": str(e)
        }, "Send Notification Request - Failed", user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error sending notification request',
                'error': str(e)
            })
        }