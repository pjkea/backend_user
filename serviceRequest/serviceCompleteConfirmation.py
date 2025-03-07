import json
import boto3
import psycopg2
import logging
import datetime
import uuid

from psycopg2.extras import DictCursor

from layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")

# Load secrets
secrets = get_secrets()

# Cofigure loggging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    conn = None
    cursor = None
    try:
        body  = json.loads(event.get('body', '{}'))
        order_id = body.get('order_id')
        user_id = body.get('user_id')
        completions_timestamp = body.get('completions_timestamp', datetime.datetime.now().isoformat())

        if not order_id or not user_id:
            raise Exception('Missing required fields: orderid and userid are required')

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        cursor.execute("""SELECT requestid FROM orders WHERE orderid = %s""", (order_id,))
        result = cursor.fetchone()

        if not result:
            raise Exception(f'Order not found: {order_id}')

        requestid = result.get('requestid')

        cursor.execute("""UPDATE requests SET status = %s, updatedat = %s WHERE requestid = %s""",
                       ('COMPLETED', completions_timestamp, requestid))

        conn.commit()

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Service completion confirmed',
                'order_id': order_id,
                'user_id': user_id,
                'timestamp': completions_timestamp
            })
        }

    except Exception as e:
        logger.error(f"Error processing service completion: {str(e)}")
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
        if cursor:
            cursor.close()
        if conn:
            conn.close()

