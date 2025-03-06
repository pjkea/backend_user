import json
import boto3
import logging
from psycopg2.extras import RealDictCursor
from serviceRequest.layers.utils import get_secrets, get_db_connection


# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS Topics
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']
USER_NOTIFICATION_TOPIC_ARN = secrets['USER_NOTIFICATION_TOPIC_ARN']


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        for record in event['Records']:
            try:
                message = json.loads(record["Sns"]["Message"])
                user_id = event.get("userid")
                sp_data = message.get("provider")
                sp_id = sp_data.get("id")
                location = sp_data.get("currentLocation")
                eta = message.get("eta")

                # Validate required fields
                if not all([user_id, sp_id, location, eta]):
                    logger.error(f"Missing required parameters: user_id={user_id}, sp_id={sp_id}")
                    raise ValueError("Missing required parameters in SNS message")

                # Get service provider info
                connection = get_db_connection()
                cursor = connection.cursor(cursor_factory=RealDictCursor)

                cursor.execute("""SELECT t.tidyspid, ud.userid, ud.firstname, ud.lastname
                FROM tidysp t JOIN userdetails ud ON t.userid = ud.userid 
                WHERE t.tidyspid = %s""",
                (sp_id,))
                sp_info = cursor.fetchone()

                if not sp_info:
                    logger.error(f"Service provider not found: {sp_id}")
                    raise ValueError(f"Service provider not found: {sp_id}")

                # Send user a notification
                sns_client.publish(
                    TopicArn=USER_NOTIFICATION_TOPIC_ARN,
                    Message=json.dumps({
                        'tidysp': sp_info,
                        'location': location,
                        'eta': eta,
                    }),
                    Subject="TidySp Info and ETA",
                )

                # Log success to SNS
                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps({
                        "logtypeid": 1,
                        "categoryid": 1,  # Service Request
                        "transactiontypeid": 12,  # Address Update(ignore)
                        "statusid": 34, # Request started,
                        "tidysp": sp_info,
                        "userid": user_id,
                    }),
                    Subject='TidySp Tracking ETA Successful',
                )
                logger.info(f"Successfully sent notification for SP: {sp_id}, User: {user_id}")

                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'status': 'success',
                        'userid': user_id,
                        'tidysp': sp_info,
                        'location': location,
                        'eta': eta,
                    })
                }

            except Exception as e:
                logger.error(f"Error processing record: {e}")

                # Log error to SNS
                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps({
                        "logtypeid": 4,
                        "categoryid": 1,  # Service Request
                        "transactiontypeid": 12,  # Address Update(ignore)
                        "statusid": 43,  # Failed,
                        "tidysp": sp_info,
                        "userid": user_id,
                    }),
                    Subject='TidySp Tracking ETA Failure',
                )

                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'status': 'error',
                        'message': str(e),
                    })
                }

    except Exception as e:
        logger.error(f"Lambda execution error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': str(e),
            })
        }

    finally:
        if connection:
            connection.close()
        if cursor:
            cursor.close()

#L2

