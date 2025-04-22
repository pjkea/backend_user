import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, get_client_ip, get_user_agent

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
USER_ACTIVITY_TOPIC_ARN = secrets["USER_ACTIVITY_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")

        body = json.loads(event.get('body', '{}'))
        activity_type = body.get('activity_type')
        details = body.get('details', {})

        # Additional information for activity context
        client_ip = get_client_ip(event)
        user_agent = get_user_agent(event)

        # Validate required parameters
        if not user_id or not activity_type:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: userid and activity_type are required'
                })
            }

        # Enrich details with client information
        enriched_details = {
            **details,
            'client_ip': client_ip,
            'user_agent': user_agent,
            'timestamp': datetime.now().isoformat()
        }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify user exists
        cursor.execute("""
            SELECT userid FROM users WHERE userid = %s
        """, (user_id,))

        if not cursor.fetchone():
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'User not found'})
            }

        # Prepare message for SNS
        message = {
            'user_id': user_id,
            'activity_type': activity_type,
            'details': enriched_details,
            'client_ip': client_ip,
            'user_agent': user_agent,
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=USER_ACTIVITY_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'User Activity: {activity_type}'
        )

        # Log success to SNS
        log_to_sns(1, 7, 11, 1, {
            "user_id": user_id,
            "activity_type": activity_type
        }, "User Activity Logged - Success", user_id)

        logger.info(f"Successfully initiated user activity logging for user {user_id}")

        return {
            'statusCode': 202,  # Accepted
            'body': json.dumps({
                'message': 'User activity logged successfully',
                'activity_type': activity_type,
                'timestamp': datetime.now().isoformat()
            })
        }

    except Exception as e:
        logger.error(f"Failed to log user activity: {e}")

        # Log error to SNS
        log_to_sns(4, 7, 11, 43, {
            "user_id": user_id,
            "activity_type": activity_type if 'activity_type' in locals() else None,
            "error": str(e)
        }, "User Activity Logging - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to log user activity',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()