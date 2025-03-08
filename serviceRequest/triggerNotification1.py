import json
import boto3
import logging

from psycopg2.extras import RealDictCursor
from layers.utils import get_secrets, get_db_connection, log_to_sns


# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS Topic
PROVIDER_ASSIGNMENT_NOTIFICATION_TOPIC_ARN = secrets['PROVIDER_ASSIGNMENT_NOTIFICATION_TOPIC_ARN']


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        body = json.loads(event.get('body', '{}'))
        userid = body['userid']
        tidyspid = body['tidyspid']

        # Get service provider info
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""SELECT t.tidyspid, ud.userid, ud.firstname, ud.lastname, ud.phonenumber
                FROM tidysp t JOIN userdetails ud ON t.userid = ud.userid 
                WHERE t.tidyspid = %s""", (tidyspid,))
        tidysp_info = cursor.fetchone()
        connection.commit()

        # Send to SNS
        sns_client.publish(
            TopicArn=PROVIDER_ASSIGNMENT_NOTIFICATION_TOPIC_ARN,
            Message=json.dumps({
                "userid": userid,
                "tidyspid": tidyspid,
                "tidyspinfo": tidysp_info
            }),
            Subject='Provider Assignment Notification',
        )

        log_to_sns(1, 26, 1, 51, tidysp_info, 'Provider Assigned', userid)

        logger.info('Sent Assignment Notification')

        return {
            'statusCode': 200,
            'body': json.dumps({
                'userid': userid,
                'tidyspid': tidyspid,
                'tidyspinfo': tidysp_info
            })
        }

    except Exception as e:
        logger.error("Failed to send Assignment Notification.", exc_info=e)

        log_to_sns(4, 26, 1, 6, tidysp_info, '', userid)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'message': e
            })
        }


# L1