import json
import boto3
import logging
import time

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
LOCATION_UPDATE_TOPIC_ARN = secrets['LOCATION_UPDATE_TOPIC_ARN']


def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))

        tracking_session = body.get('tracking_session')
        tidyspid = body.get('tidyspid')
        userid = body.get('userid')
        orderid = body.get('orderid')
        current_location = tracking_session.get('current_location')

        if not all([tracking_session, tidyspid, userid, orderid, current_location]):
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameters'})
            }

        timestamp = int(time.time())
        location_data = {
            'latitude': current_location.get('latitude'),
            'longitude': current_location.get('longitude'),
            'timestamp': timestamp,
        }

        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""SELECT is_paused FROM tracking WHERE orderid = %s""", (orderid,))
        paused = cursor.fetchone()

        if paused.get('is_paused', False):
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Tracking is currently paused','orderid': orderid})
            }

        sns_client.publish(
            TopicArn=LOCATION_UPDATE_TOPIC_ARN,
            Message=json.dumps({
                'locationdata': location_data,
                'orderid': orderid,
                'userid': userid,
                'tidyspid': tidyspid,
            })
        )

        logger.info('Location updated successfully')

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Location update published successfully',
                'timestamp': timestamp
            })
        }

    except Exception as e:
        logger.error(f'Failed to update location: {e}')
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Error updating location: {str(e)}'})
        }

    finally:
        cursor.close()
        connection.close()
