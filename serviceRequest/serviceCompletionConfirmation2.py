import json
import boto3
import logging
import psycopg2
import time

from layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']


def send_feedback_request(user_id, service_id, service_details):
    """Helper function to send feedback request"""
    logger.info(f"Sending feedback request to user {user_id} for service {service_id}")

    return {
        'userid': user_id,
        'serviceid': service_id,
        'servicedetails': service_details
    }


def update_service_analytics(service_id, user_id, timestamp, service_details):
    """Helper function to update service analytics"""
    logger.info(f"Updating analytics for service {service_id}")

    return {
        'userid': user_id,
        'serviceid': service_id,
        'servicedetails': service_details,
        'timestamp': timestamp,
    }


def lambda_handler(event, context):
    try:
        logger.info(f"Received event: {json.dumps(event)}")

        for record in event.get('Records', []):
            message = json.loads(record['Sns']['Message'])
            order_details = message.get('orderdetails', {})
            actions = message.get('actions', {})

            # Handle both array of records and single record
            if isinstance(order_details, list) and order_details:
                first_record = order_details[0]
                order_id = first_record.get('orderid')
                user_id = first_record.get('userid')
            else:
                order_id = order_details.get('orderid')
                user_id = order_details.get('userid')

            completion_timestamp = message.get('timestamp')

            results = {
                'feedbackRequestSent': False,
                'analyticsUpdated': False,
                'userHistoryUpdated': False
            }

            if actions.get('sendFeedbackRequest'):
                send_feedback_request(user_id, order_id, order_details)
                results['feedbackRequestSent'] = True

            if actions.get('updateAnalytics'):
                update_service_analytics(user_id, order_id, completion_timestamp)
                results['analyticsUpdated'] = True

            if actions.get('updateUserHistory'):
                results['userHistoryUpdated'] = True

            # Log to SNS
            sns_client.publish(
                TopicArn=SNS_LOGGING_TOPIC_ARN,
                Message=json.dumps({
                    "logtypeid": 1,
                    "categoryid": 11,  # Service Completion
                    "transactiontypeid": 12,  # Address Update(ignore)
                    "statusid": 47,  # Service Request Completed
                    "orderid": order_id,
                    "userid": user_id,
                    "timestamp": completion_timestamp,
                }),
                Subject='Lambda 2 - Service Completions',
            )

            logger.info(f"Async processing results for service {order_id}: {results}")

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Async processing completed successfully'})
        }

    except Exception as e:
        logger.error(f"Lambda 2 - Error in async processing: {str(e)}")

        # Log error to sns
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 4,
                "categoryid": 11,  # Service Completion
                "transactiontypeid": 12,  # Address Update(ignore)
                "statusid": 43, # Failure
                "orderid": order_id,
                "userid": user_id,
                "timestamp": completion_timestamp,
            }),
            Subject='Async processing completed with errors',
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Async processing completed with errors',
                'error': str(e)
            })
        }





#3


















