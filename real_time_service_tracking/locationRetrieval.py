import json
import boto3
import logging

from psycopg2.extras import RealDictCursor
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

        orderid = body.get('orderid', '')
        userid = body.get('userid', '')
        location = body.get('locationdata', {})

        if not orderid:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': f'Missing {orderid}'})
            }

        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""SELECT tidyspid, status, splocation, is_paused, history, updatedat 
        FROM tracking WHERE orderid = %s""", (orderid,))

        data = cursor.fetchone()

        if data is None:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': f'Order does not exist {orderid}'})
            }

        logger.info(f"Successfully retrieved {orderid}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'orderid': orderid,
                'userid': userid,
                'tidyspid': data.get('tidyspid'),
                'current_location': data.get('currentlocation', {}),
                'is_paused': data.get('is_paused', False),
                'history': data.get('history', []),
                'updatedat': data.get('updatedat'),
            })
        }

    except Exception as e:
        logger.error(f"Unexpected error: {e}")

        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Error retrieving location: {str(e)}'})
        }

    finally:
        cursor.close()
        connection.close()
