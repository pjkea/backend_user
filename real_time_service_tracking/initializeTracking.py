import json
from json import JSONDecoder

import boto3
import logging
import uuid
import time

from datetime import datetime
from psycopg2.extras import RealDictCursor, Json
from layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))

        tidyspid = body.get('tidyspid')
        userid = body.get('userid')
        orderid = body.get('orderid')
        initial_location = body.get('current_location')

        if not all([tidyspid, userid, orderid, initial_location]):
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameters'})
            }

        tracking_session_id = str(uuid.uuid4())
        timestamp = int(time.time())
        timestamp = datetime.fromtimestamp(timestamp)

        location = {
            'latitude': initial_location.get('latitude'),
            'longitude': initial_location.get('longitude'),
            'timestamp': timestamp
        }

        tracking_session = {
            'tracking_session_id': tracking_session_id,
            'tidyspid': tidyspid,
            'userid': userid,
            'orderid': orderid,
            'status': 'active',
            'current_location': location,
            'created_at': timestamp,
            'updated_at': timestamp,
            'is_paused': False
        }

        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""INSERT INTO tracking 
        (tidyspid, userid, orderid, status, splocation, is_paused, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW()) RETURNING trackingid""", (tidyspid, userid, orderid, 'active', location, False))

        cursor.execute("""INSERT INTO trackinghistory (trackingid, status, timestamp, data, userid, orderid, tidyspid)
        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (tracking_session_id,'initialized', timestamp, Json({'initial_location': location, 'event': 'tracking_started'}), userid, orderid, tidyspid))

        connection.commit()

        logger.info(f"Tracking session initialized successfuly, {tracking_session}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Tracking session initiated successfully',
                'userid': userid,
                'tidyspid': tidyspid,
                'orderid': orderid,
                'tracking_session': tracking_session,
                'timestamp': timestamp
            })
        }

    except Exception as e:
        logger.error(f"Failed to initialize tracking session: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error initializing tracking session: {str(e)}'
            })
        }

    finally:
        cursor.close()
        connection.close()
