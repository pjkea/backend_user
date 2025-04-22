import json
import boto3
from datetime import datetime
import logging

from psycopg2.extras import RealDictCursor
from layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topic
TRACKING_END_TOPIC_ARN = secrets['TRACKING_END_TOPIC_ARN']


def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))

        orderid = body.get('orderid')
        userid = body.get('userid')
        tidyspid = body.get('tidyspid')
        completedby = body.get('completedby')

        if not all([orderid, userid, tidyspid, completedby]):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing required parameters'})
            }

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("SELECT status FROM orders WHERE order_id = %s AND user_id = %s", (orderid, userid))

        order = cursor.fetchone()
        if order is None:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Order not found'})
            }

        status = order['status']

        if status == 5:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Order already completed'})
            }

        tracking_data = {
            'orderid': orderid,
            'userid': userid,
            'tidyspid': tidyspid,
            'completedby': completedby,
            'timecompleted': datetime.now().isoformat()
        }

        sns_client.publish(
            TopicArn=TRACKING_END_TOPIC_ARN,
            Message=json.dumps(tracking_data),
            Subject=f'Tracking Session End - Order {orderid}'
        )
        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Tracking session end initiated for order {orderid}',
                'order_id': orderid,
                'status': 'processing'
            })
        }

    except Exception as e:
        logger.error(f'Unexpected error: {e}')

        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Internal server error: {str(e)}'})
        }
