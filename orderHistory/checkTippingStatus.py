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

        # Check if the order is completed (only completed orders can be tipped)
        if order['status'] != 'COMPLETED':
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Order is not completed yet, tipping not available',
                    'status': order['status']
                })
            }

        # Determine if user is a customer or service provider
        is_customer = order['userid'] == user_id

        # If the user is not a customer, they cannot tip
        if not is_customer:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Only customers can tip service providers',
                    'is_customer': False
                })
            }

        # Check if user has already submitted a tip
        cursor.execute("""
            SELECT paymentid, amount, createdat, status
            FROM payments
            WHERE orderid = %s AND payment_type = 'TIP'
        """, (order_id,))

        existing_tip = cursor.fetchone()

        # Format response data
        tipping_status = {
            'order_id': order_id,
            'order_status': order['status'],
            'completed_at': order['completedat'].isoformat() if order['completedat'] else None,
            'is_tippable': True,
            'user_has_tipped': existing_tip is not None
        }

        if existing_tip:
            formatted_tip = dict(existing_tip)
            for key, value in formatted_tip.items():
                if isinstance(value, datetime):
                    formatted_tip[key] = value.isoformat()

            tipping_status['tip_details'] = formatted_tip

        # Log success to SNS
        log_to_sns(1, 11, 5, 1, {
            "user_id": user_id,
            "order_id": order_id,
            "has_tipped": existing_tip is not None
        }, "Check Tipping Status - Success", user_id)

        logger.info(f"Successfully checked tipping status for order {order_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Tipping status retrieved successfully',
                'tipping_status': tipping_status
            })
        }

    except Exception as e:
        logger.error(f"Failed to check tipping status: {e}")

        # Log error to SNS
        log_to_sns(4, 11, 5, 43, {
            "user_id": user_id,
            "order_id": order_id,
            "error": str(e)
        }, "Check Tipping Status - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to check tipping status',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()