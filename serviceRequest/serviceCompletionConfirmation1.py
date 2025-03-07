import json
import boto3
import logging
import psycopg2
import datetime
import uuid

from psycopg2.extras import RealDictCursor

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
SERVICE_COMPLETION_TOPIC_ARN = secrets['SERVICE_COMPLETION_TOPIC_ARN']


def lambda_handler(event, context):
    conn = None
    cursor = None
    try:
        logger.info(f"lambda 1 - Received event: {event}")

        body = json.loads(event.get('body', '{}'))
        order_id = body.get('order_id')
        user_id = body.get('user_id')
        completion_timestamp = body.get('timestamp', datetime.datetime.now().isoformat())

        if not order_id or not user_id:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required fields: serviceId and userId are required'
                })
            }

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)


        # Retrieve order details
        cursor.execute("""SELECT o.userid o.tidyspid, o.totalprice, od.add_ons 
        FROM orders o 
        JOIN orderdetails od ON o.orderid = od.orderid 
        WHERE o.orderid = %s""", (order_id,))
        order_records = cursor.fetchall()

        if not order_records:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'message': f'Order {order_id} not found'
                })
            }

        # Change status in orders table
        cursor.execute("""UPDATE orders SET status = %s, updatedat = %s WHERE orderid = %s""",
                       ('COMPLETED', completion_timestamp, order_id))

        conn.commit()

        message = {
            'orderdetails': order_records,
            'actions': {
                'sendFeedbackRequest': True,
                'updateAnalytics': True,
                'updateUserHistory': True
            },
            'timestamp': completion_timestamp
        }

        # Send to SNS
        sns_client.publish(
            TopicArn=SERVICE_COMPLETION_TOPIC_ARN,
            Message=json.dumps(message),
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Service completion confirmed',
                'orderid': order_id,
                'timestamp': completion_timestamp,
                'note': 'Feedback request will be sent shortly'
            })
        }

    except Exception as e:
        logger.error(f"Lambda 1 -  Error processing service completion: {str(e)}")
        if conn:
            conn.rollback()

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error confirming service completion',
                'error': str(e)
            })
        }

    finally:
        if conn:
            conn.close()
        if cursor:
            cursor.close()




#2






