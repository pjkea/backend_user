import json
import boto3
import logging

from datetime import datetime
from psycopg2.extras import RealDictCursor, Json
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
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        for record in event['Records']:
            try:
                sns_message = json.loads(record['Sns']['Message'])

                trackingid = sns_message.get('trackingid')
                tidyspid = sns_message.get('tidyspid')
                userid = sns_message.get('userid')
                orderid = sns_message.get('orderid')
                status_type = sns_message.get('type')
                location = sns_message.get('location')
                timestamp = sns_message.get('timestamp')

                if not all([trackingid, tidyspid, userid, orderid, status_type, location, timestamp]):
                    print(f"Missing data in status update: {sns_message}")
                    continue

                cursor.execute("UPDATE tracking SET status = %s, updatedat =%s WHERE trackingid = %s ",
                               (status_type, datetime.fromtimestamp(timestamp), trackingid))

                cursor.execute("""
                    INSERT INTO tracking_history 
                    (trackingid, status, timestamp, location, userid, orderid, tidyspid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,(trackingid,status_type,datetime.fromtimestamp(timestamp),
                        Json({location}),userid,orderid,tidyspid))

                conn.commit()

                log_to_sns(1, 2, 3, 4, "Updated status update", '', userid)
                logger.info(f"Updated status update: {trackingid}, {status_type}")

            except Exception as record_error:
                logger.error(f"Error processing record {record}: {str(record_error)}")
                conn.rollback()
                log_to_sns(1, 2, 3, 4, str(record_error), '', userid)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Events processed successfully'})
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Internal server error: {str(e)}'})
        }

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()






