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


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")

        body = json.loads(event.get('body', '{}'))
        order_id = body.get('order_id')
        message_id = body.get('message_id')

        # Validate required parameters
        if not user_id or not order_id or not message_id:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: userid, order_id, and message_id are required'
                })
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify that the message belongs to the specified order and is a thank-you note
        cursor.execute("""
            SELECT m.*, o.tidyspid 
            FROM inappmessages m
            JOIN orders o ON m.orderid = o.orderid
            WHERE m.messageid = %s AND m.orderid = %s AND m.receiverid = %s
        """, (message_id, order_id, user_id))

        message = cursor.fetchone()
        if not message:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'message': 'Message not found or not authorized to like this message'
                })
            }

        # Check if the message is already liked
        cursor.execute("""
            SELECT likeid FROM message_likes 
            WHERE messageid = %s AND userid = %s
        """, (message_id, user_id))

        existing_like = cursor.fetchone()

        # If already liked, we can either toggle it off or leave it liked
        if existing_like:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'You have already liked this thank-you note',
                    'like_id': existing_like['likeid']
                })
            }

        # Add the like
        cursor.execute("""
            INSERT INTO message_likes (messageid, userid, createdat) 
            VALUES (%s, %s, NOW()) 
            RETURNING likeid
        """, (message_id, user_id))

        like_result = cursor.fetchone()
        like_id = like_result['likeid']

        # Update like count in the messages table if it has such a column
        try:
            cursor.execute("""
                UPDATE inappmessages 
                SET like_count = COALESCE(like_count, 0) + 1 
                WHERE messageid = %s
            """, (message_id,))
        except Exception as e:
            # If the column doesn't exist, we can ignore this error
            logger.warning(f"Could not update like_count: {e}")

        # Send a notification to the TidySP
        tidysp_id = message['tidyspid']
        senderid = message['senderid']

        cursor.execute("""
            INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
            VALUES (%s, %s, %s, %s, NOW())
        """, (senderid, 'LIKE', f"User liked your thank-you note for order #{order_id}", False))

        connection.commit()

        # Log success to SNS
        log_to_sns(1, 11, 19, 1, {
            "user_id": user_id,
            "order_id": order_id,
            "message_id": message_id,
            "like_id": like_id
        }, "Like Thank You Note - Success", user_id)

        logger.info(f"Successfully liked thank-you note in message {message_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Thank you note liked successfully',
                'order_id': order_id,
                'message_id': message_id,
                'like_id': like_id
            })
        }

    except Exception as e:
        logger.error(f"Failed to like thank-you note: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 11, 19, 43, {
            "user_id": user_id,
            "order_id": order_id,
            "message_id": message_id,
            "error": str(e)
        }, "Like Thank You Note - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to like thank-you note',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()