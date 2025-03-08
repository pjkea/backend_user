import json
import boto3
import logging
from twilio.rest import Client
from serviceRequest.layers.utils import get_secrets, send_email_via_ses, send_sms_via_twilio, log_to_sns

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


def prepare_notification_message(record, service_details):
    """Prepare a standardized notification message from a record"""
    # Get the package and price details
    package = service_details.get('package', {})
    price = service_details.get('price', {})
    address = service_details.get('address', 'N/A')
    total_price = f"{price.get('totalPrice', 0)} {price.get('currency', 'USD')}"

    # Construct a standardized notification message
    subject = "New Service Request - {package.get('name', 'Service')}"
    message = (
        f"New Service Request Available!\n\n"
        f"Service: {package.get('name', 'N/A')}\n"
        f"Distance: {record.get('distance_km', 'N/A')} km\n"
        f"Location: {address}\n"
        f"Distance from you: {record.get('distance_km', 'N/A')} km\n\n"
        f"Total Price: {total_price}\n"
        "If you want to accept the request, please open the app."
    )

    return subject, message



def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            message = json.loads(record["Sns"]["Message"])

            requestid = message.get('requestid')
            userid = message.get('userid')
            service_details = message.get('service_details', {})

            # Extract nearby records
            nearby_sp = message.get('available_sp', [])

            notification_sent = 0
            notification_failed = 0

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

                    notification_sent += 1

                except Exception as record_error:
                    logger.error(f"Failed to process record: {record_error}")
                    continue

            data = {
                "requestid": requestid,
                "notifications_sent": notification_sent,
                "notifications_failed": notification_failed
            }
            # Log success to SNS
            log_to_sns(1, 36, 12, 4, data, "Provider Notification - Success", userid)

            logger.info("Successfully sent notifications")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Notifications processed successfully',
                    'total_notifications': len(nearby_sp),
                    'sent': notification_sent,
                    'failed': notification_failed
                })
            }

    except Exception as e:
        logger.error(f"Notification processing error: {e}")

        # Log error to SNS
        log_to_sns(4, 36, 12, 4, e, "Provider Notification - Failed", userid)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to process notifications',
                'message': str(e)
            })
        }

