import json
import boto3
import logging
from twilio.rest import Client
from serviceRequest.layers.utils import get_secrets, send_email_via_ses, send_sms_via_twilio

# Initialize AWS services
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")
ses_client = boto3.client("ses", region_name="us-east-1")

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS Topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
NOTIFY_PROVIDER_TOPIC_ARN = secrets["NOTIFY_PROVIDER_TOPIC_ARN"]

# Twilio details
TWILIO_ACCOUNT_SID = secrets["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = secrets["TWILIO_AUTH_TOKEN"]
TWILIO_PHONE_NUMBER = secrets["TWILIO_PHONE_NUMBER"]

# Initialize Twilio Client
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def prepare_notification_message(record):
    """Prepare a standardized notification message from a record"""
    try:
        # Construct a standardized notification message
        subject = "Nearby Service Request"
        message = (
            f"Nearby Service Provider Details:\n"
            f"Name: {record.get('firstname', '')} {record.get('lastname', '')}\n"
            f"Distance: {record.get('distance_km', 'N/A')} km\n"
            f"TidySpID: {record.get('tidyspid', 'N/A')}"
            "If you want to accept the request please open the app."
        )

        return subject, message
    except Exception as e:
        logger.error(f"Failed to prepare notification message: {e}")
        raise


def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            message = json.loads(record["Sns"]["Message"])

            # Extract nearby records
            nearby_sp = message.get('available_sp', [])

            # Process each nearby record
            for sp in nearby_sp:
                try:
                    # Prepare notification message
                    subject, notification_text = prepare_notification_message(sp)

                    # Send email if email exists
                    if record.get('email'):
                        send_email_via_ses(
                            sp['email'],
                            subject,
                            notification_text
                        )

                    # Send SMS if phone number exists
                    if record.get('phone_number'):
                        send_sms_via_twilio(
                            sp['phone_number'],
                            notification_text
                        )

                except Exception as record_error:
                    logger.error(f"Failed to process record: {record_error}")
                    # Continue processing other records
                    continue

            # Log success to SNS
            sns_client.publish(
                TopicArn=SNS_LOGGING_TOPIC_ARN,
                Message=json.dumps({
                    "logtypeid": 1,
                    "categoryid": 36,  # Service Assignment
                    "transactiontypeid": 12,  # Address Update(ignore)
                    "statusid": 5,  # Service Request Assigned
                })
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Notifications processed successfully',
                    'total_notifications': len(nearby_sp)
                })
            }

    except Exception as e:
        logger.error(f"Notification processing error: {e}")

        # Log error to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 4,
                "categoryid": 36,  # Service Assignment
                "transactiontypeid": 12,  # Address Update(ignore)
                "statusid": 4,  # Service Request Failed
            })
        )

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to process notifications',
                'message': str(e)
            })
        }













