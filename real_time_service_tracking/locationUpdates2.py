import json
import boto3
import logging
import time

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
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            location_data = message.get('locationdata')
            orderid = message.get('orderid')
            userid = message.get('userid')
            tidyspid = message.get('tidyspid')

            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            cursor.execute("""UPDATE tracking SET splocation = %s, updatedat = NOW()
                WHERE orderid = %s""", (location_data, orderid))
            connection.commit()

            log_to_sns(1,2,3,4,location_data,"", userid)

            logger.info("Updated location for tracking session")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Location Updated',
                    'locationdata': location_data,
                    'timestamp': int(time.time()),
                    'orderid': orderid,
                    'userid': userid,
                    'tidyspid': tidyspid,
                })
            }

    except Exception as e:
        logger.error(f"Error processing location updates: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error processing location updates: {str(e)}'
            })
        }

    finally:
        cursor.close()
        connection.close()