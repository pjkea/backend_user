import json
import boto3
import time
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
STATUS_UPDATE_TOPIC_ARN = secrets['STATUS_UPDATE_TOPIC_ARN']


def lambda_handler(event, context):
    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        body = json.loads(event.get('body', '{}'))

        orderid = body.get('orderid')
        trackingid = body.get('trackingid')
        status = body.get('status', '')
        tidyspid = body.get('tidyspid')

        if not all([orderid, trackingid, status, tidyspid]):
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters'
                })
            }

        cursor.execute("""SELECT * FROM tracking WHERE trackingid = %s""", (trackingid,))
        result = cursor.fetchone()

        if not result:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'message': 'Tracking session not found'
                })
            }

        status_types = ['started_driving','near_location','arrived','service_started','service_completed','delay_reported','custom_update']

        if status not in status_types:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': f'Invalid status type. Must be one of: {", ".join(status_types)}'
                })
            }

        timestamp = int(time.time())
        status_record = {
            'type': status,
            'timestamp': timestamp,
            'trackingid': trackingid,
            'userid': body.get('userid'),
            'orderid': orderid,
            'tidyspid': tidyspid,
            'location': result.get('currentlocation'),
        }

        sns_client.publish(
            TopicArn=STATUS_UPDATE_TOPIC_ARN,
            Message=json.dumps(status_record),
        )

        logger.info(f'Status updated successfully: {trackingid}')

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Status update published successfully',
                'tracking_session': trackingid,
                'status_record': status_record,
                'timestamp': timestamp
            })
        }

    except Exception as e:
        logger.error(f'Error updating tracking session: {e}')

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error updating tracking status: {str(e)}'
            })
        }

    finally:
        cursor.close()
        connection.close()
