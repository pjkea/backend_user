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
        tidysp_id = query_params.get("tidyspid")

        body = json.loads(event.get('body', '{}'))
        order_id = body.get('order_id')
        time_type = body.get('time_type')  # "accepted" or "completed"
        timestamp = body.get('timestamp', datetime.now().isoformat())

        # Validate required parameters
        if not tidysp_id or not order_id or not time_type:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: tidyspid, order_id, and time_type are required'
                })
            }

        # Validate time_type
        if time_type.lower() not in ['accepted', 'completed']:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'time_type must be either "accepted" or "completed"'
                })
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify the TidySP is assigned to this order
        cursor.execute("""
            SELECT orderid, status FROM orders 
            WHERE orderid = %s AND tidyspid = %s
        """, (order_id, tidysp_id))

        order = cursor.fetchone()
        if not order:
            return {
                'statusCode': 403,
                'body': json.dumps({
                    'message': 'Unauthorized: TidySP is not assigned to this order'
                })
            }

        # Handle different time types
        if time_type.lower() == 'accepted':
            # Check if the order is in a valid state to be accepted
            if order['status'] not in ['PENDING', 'ASSIGNED']:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'message': f'Cannot update accepted time for order in {order["status"]} status'
                    })
                }

            # Update the order status and accepted time
            cursor.execute("""
                UPDATE orders 
                SET status = 'ACCEPTED', acceptedat = %s, updatedat = NOW() 
                WHERE orderid = %s
            """, (timestamp, order_id))

            # Insert status history record
            cursor.execute("""
                INSERT INTO orderstatushistory (orderid, status, createdat) 
                VALUES (%s, 'ACCEPTED', %s)
            """, (order_id, timestamp))

            status_update = 'ACCEPTED'

        else:  # time_type == 'completed'
            # Check if the order is in a valid state to be completed
            if order['status'] not in ['ACCEPTED', 'IN_PROGRESS']:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'message': f'Cannot update completed time for order in {order["status"]} status'
                    })
                }

            # Update the order status and completed time
            cursor.execute("""
                UPDATE orders 
                SET status = 'COMPLETED', completedat = %s, updatedat = NOW() 
                WHERE orderid = %s
            """, (timestamp, order_id))

            # Insert status history record
            cursor.execute("""
                INSERT INTO orderstatushistory (orderid, status, createdat) 
                VALUES (%s, 'COMPLETED', %s)
            """, (order_id, timestamp))

            status_update = 'COMPLETED'

        connection.commit()

        # Log success to SNS
        log_to_sns(1, 11, 8, 1, {
            "tidysp_id": tidysp_id,
            "order_id": order_id,
            "time_type": time_type,
            "timestamp": timestamp
        }, f"Update Order Time - {status_update}", tidysp_id)

        logger.info(f"Successfully updated {time_type} time for order {order_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Order {time_type} time updated successfully',
                'order_id': order_id,
                'status': status_update,
                'timestamp': timestamp
            })
        }

    except Exception as e:
        logger.error(f"Failed to update order time: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 11, 8, 43, {
            "tidysp_id": tidysp_id,
            "order_id": order_id,
            "time_type": time_type,
            "error": str(e)
        }, "Update Order Time - Failed", tidysp_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Failed to update {time_type} time',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()