import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns

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
DATA_MANAGEMENT_TOPIC_ARN = secrets["DATA_MANAGEMENT_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")

        body = json.loads(event.get('body', '{}'))
        request_type = body.get('request_type')  # 'export' or 'delete'
        reason = body.get('reason', '')

        # Validate required parameters
        if not user_id or not request_type:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: userid and request_type are required'
                })
            }

        # Validate request type
        if request_type not in ['export', 'delete']:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Invalid request_type. Must be either "export" or "delete"'
                })
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify user exists
        cursor.execute("""
            SELECT u.userid, u.email, ud.firstname, ud.lastname
            FROM users u
            LEFT JOIN userdetails ud ON u.userid = ud.userid
            WHERE u.userid = %s
        """, (user_id,))

        user = cursor.fetchone()

        if not user:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'User not found'})
            }

        # Get client IP for security logging
        client_ip = event.get('requestContext', {}).get('identity', {}).get('sourceIp', 'unknown')

        # Create data request record in the database
        cursor.execute("""
            INSERT INTO data_requests (userid, requesttype, reason, status, createdat)
            VALUES (%s, %s, %s, %s, NOW())
            RETURNING requestid
        """, (user_id, request_type, reason, 'PENDING'))

        request_result = cursor.fetchone()
        request_id = request_result['requestid']

        # Log the data request in the activity logs
        cursor.execute("""
            INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
            VALUES (%s, %s, %s, %s, NOW())
        """, (
            user_id,
            f'DATA_{request_type.upper()}_REQUESTED',
            json.dumps({
                'request_id': request_id,
                'reason': reason,
                'client_ip': client_ip,
                'timestamp': datetime.now().isoformat()
            }),
            client_ip
        ))

        connection.commit()

        # Prepare message for SNS
        message = {
            'request_id': request_id,
            'user_id': user_id,
            'email': user.get('email'),
            'name': f"{user.get('firstname', '')} {user.get('lastname', '')}",
            'request_type': request_type,
            'reason': reason,
            'client_ip': client_ip,
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=DATA_MANAGEMENT_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'Data {request_type.capitalize()} Request: {request_id}'
        )

        # Log success to SNS
        log_to_sns(1, 7, 9, 1, {
            "user_id": user_id,
            "request_id": request_id,
            "request_type": request_type
        }, f"Data {request_type.capitalize()} Requested - Success", user_id)

        logger.info(f"Successfully initiated data {request_type} request {request_id} for user {user_id}")

        # Create response with appropriate message based on request type
        response_messages = {
            'export': 'Your data export request has been submitted. You will receive your data via email once processed.',
            'delete': 'Your data deletion request has been submitted and is being processed. This process may take up to 30 days to complete.'
        }

        return {
            'statusCode': 202,  # Accepted
            'body': json.dumps({
                'message': response_messages.get(request_type),
                'request_id': request_id,
                'request_type': request_type,
                'status': 'PENDING'
            })
        }

    except Exception as e:
        logger.error(f"Failed to process data management request: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 9, 43, {
            "user_id": user_id,
            "request_type": request_type if 'request_type' in locals() else None,
            "error": str(e)
        }, "Data Management Request - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process data management request',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()