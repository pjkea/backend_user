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
        # Parse query parameters
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")
        order_id = query_params.get("orderid")

        # Validate required parameters
        if not user_id or not order_id:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: userid and orderid are required'
                })
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # First verify that the user has access to this order
        cursor.execute("""
            SELECT o.orderid, o.status, o.userid, o.tidyspid, o.completedat
            FROM orders o 
            WHERE o.orderid = %s AND (o.userid = %s OR o.tidyspid = %s)
        """, (order_id, user_id, user_id))

        order = cursor.fetchone()
        if not order:
            return {
                'statusCode': 403,
                'body': json.dumps({
                    'message': 'Unauthorized access to this order'
                })
            }

        # Check if the order is completed (only completed orders can be rated)
        if order['status'] != 'COMPLETED':
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Order is not completed yet, rating not available',
                    'status': order['status']
                })
            }

        # Check if user has already submitted a rating
        cursor.execute("""
            SELECT reviewid, rating, comments, createdat
            FROM reviews
            WHERE orderid = %s AND userid = %s
        """, (order_id, user_id))

        existing_review = cursor.fetchone()

        # Determine if user is a customer or service provider
        is_customer = order['userid'] == user_id

        # Check if the other party has submitted a rating
        other_user_id = order['tidyspid'] if is_customer else order['userid']

        cursor.execute("""
            SELECT reviewid, rating, comments, createdat
            FROM reviews
            WHERE orderid = %s AND userid = %s
        """, (order_id, other_user_id))

        other_review = cursor.fetchone()

        # Format response data
        rating_status = {
            'order_id': order_id,
            'order_status': order['status'],
            'completed_at': order['completedat'].isoformat() if order['completedat'] else None,
            'is_ratable': True,
            'user_has_rated': existing_review is not None,
            'other_party_has_rated': other_review is not None
        }

        if existing_review:
            formatted_review = dict(existing_review)
            for key, value in formatted_review.items():
                if isinstance(value, datetime):
                    formatted_review[key] = value.isoformat()

            rating_status['user_rating'] = formatted_review

        # Log success to SNS
        log_to_sns(1, 11, 5, 1, {
            "user_id": user_id,
            "order_id": order_id,
            "has_rated": existing_review is not None
        }, "Check Rating Status - Success", user_id)

        logger.info(f"Successfully checked rating status for order {order_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Rating status retrieved successfully',
                'rating_status': rating_status
            })
        }

    except Exception as e:
        logger.error(f"Failed to check rating status: {e}")

        # Log error to SNS
        log_to_sns(4, 11, 5, 43, {
            "user_id": user_id,
            "order_id": order_id,
            "error": str(e)
        }, "Check Rating Status - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to check rating status',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()