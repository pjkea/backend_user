import json
import boto3
import logging

from datetime import datetime
from psycopg2.extras import RealDictCursor
from layers.utils import get_secrets, get_db_connection, log_to_sns

# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topic
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']


def lambda_handler(event, context):
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            orderid = message.get('orderid')
            userid = message.get('userid')
            tidyspid = message.get('tidyspid')
            completedby = message.get('completedby')
            completion_time = message.get('timecompleted')

            if not all([orderid, userid, tidyspid, completedby, completion_time]):
                logger.error("Missing required parameters in SNS message")
                return

            try:
                note = f"Service marked as completed by {completedby} at {completion_time}"
                cursor.execute("""UPDATE orders SET status = 5, updatedat = %s WHERE orderid = %s""",
                               (datetime.utcnow(), orderid))

                cursor.execute("""INSERT INTO ordernotes (orderid, note, createdat) SELECT od.orderid, %s, %s
                FROM ordernotes od WHERE od.orderid = %s LIMIT 1""", (note, datetime.utcnow(), orderid))

                cursor.execute("""UPDATE tracking SET status = %s, updatedat = %s WHERE orderid = %s AND status = 'active'""",
                               (completedby, datetime.utcnow(), orderid))

                note = f"Order {orderid} completed at {completion_time}"
                cursor.execute("""INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                VALUES (%s, %s, %s, %s, %s)""", (userid, 'service completion'), note, 0, datetime.utcnow())

                connection.commit()
                cursor.close()
                connection.close()

                log_to_sns(1,2,3,4,note, 'Service Completion', userid)
                logger.info(f"Ended tracking for Order {orderid}")

            except Exception as e:
                connection.rollback()
                logger.error(f"Failed to end tracking Order {orderid}: {e}")
                return e

    except Exception as e:
        logger.error(f"Failed to end tracking Order {orderid}: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({f'Unexpected error: {e}'}),
        }



