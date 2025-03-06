import json
import psycopg2
import logging
import boto3
from psycopg2.extras import RealDictCursor

# Use absolute imports instead of relative imports
from serviceRequest.layers.utils import get_secrets, get_db_connection
from twilio.rest import Client

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')
ses_client = boto3.client("ses", region_name="us-east-1")

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS Topic
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']

# Twilio credentials
TWILIO_ACCOUNT_SID = secrets["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = secrets["TWILIO_AUTH_TOKEN"]
TWILIO_PHONE_NUMBER = secrets["TWILIO_PHONE_NUMBER"]

# Initialize Twilio Client
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def send_sms_via_twilio(phone_number, message):
    """Sends an SMS using Twilio all available service providers"""
    try:
        # Ensure phone number is in E.164 format
        if not phone_number.startswith('+'):
            phone_number = f"+{phone_number}"

        twilio_message = twilio_client.messages.create(
            body=message,
            from_=TWILIO_PHONE_NUMBER,
            to=phone_number
        )
        logger.info(f"SMS sent via Twilio: {twilio_message.sid}")
        return "SMS Sent via Twilio"
    except Exception as twilio_error:
        logger.warning(f"Twilio SMS failed: {twilio_error}")
        raise


def send_email_via_ses(email, subject, message):
    """Sends an OTP email using AWS SES."""
    try:
        # Configurable sender email
        sender_email = "no-reply@yourdomain.com"
        html_message = message.replace('\n', '<br>')

        # Send email
        ses_client.send_email(
            Source=sender_email,
            Destination={"ToAddresses": [email]},
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Text": {"Data": message},
                    "Html": {"Data": f"<p>{html_message}</p>"}  # Proper HTML formatting
                }
            }
        )

        logger.info(f"Email sent to {email}")
        return "Email Sent Successfully"
    except Exception as e:
        logger.error(f"Failed to send email to {email}: {e}")
        raise


def prepare_notification_message(tidysp_info):
    """Prepare a standardized notification message from a record"""
    try:
        # Construct a standardized notification message
        subject = "Service Provider Assigned"

        # Properly formatted message with line breaks
        message = (
            "Your service request has been accepted by a service provider.\n\n"
            f"TidySpID: {tidysp_info.get('tidyspid', 'N/A')}\n"
            f"Name: {tidysp_info.get('firstname', '')} {tidysp_info.get('lastname', '')}\n"
        )

        return subject, message
    except Exception as e:
        logger.error(f"Failed to prepare notification message: {e}")
        raise


def lambda_handler(event, context):
    connection = None
    cursor = None

    # Track all processed records
    results = []

    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        for record in event['Records']:
            record_result = {
                'success': False,
                'record_id': record.get('messageId', 'unknown')
            }

            try:
                message = json.loads(record['Sns']['Message'])

                # Extract data with proper validation
                userid = message.get('userid')
                tidyspid = message.get('tidyspid')
                # Use correct key, assuming 'tidysp' is the correct key based on previous examples
                tidysp_info = message.get('tidysp') or message.get('tidyspinfo', {})

                if not all([userid, tidyspid, tidysp_info]):
                    raise ValueError(f"Missing required fields in message: {message}")

                # Get user contact
                cursor.execute("""
                    SELECT ud.phonenumber, u.email 
                    FROM users u 
                    JOIN userdetails ud ON u.userid = ud.userid 
                    WHERE u.userid = %s
                """, (userid,))

                user_info = cursor.fetchone()

                if user_info is None:
                    raise ValueError(f"User not found: {userid}")

                user_number = user_info.get('phonenumber')
                user_email = user_info.get('email')

                # Verify at least one contact method exists
                if not user_number and not user_email:
                    raise ValueError(f"No contact information found for user: {userid}")

                # Prepare notification message
                subject, message_text = prepare_notification_message(tidysp_info)

                # Track what was sent
                notification_sent = []

                # Send email if available
                if user_email:
                    send_email_via_ses(user_email, subject, message_text)
                    notification_sent.append("email")

                # Send SMS if available
                if user_number:
                    send_sms_via_twilio(user_number, message_text)
                    notification_sent.append("sms")

                # Log success to SNS
                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps({
                        "logtypeid": 1,
                        "categoryid": 36,  # Service Assignment
                        "transactiontypeid": 12,  # Address Update(ignore)
                        "statusid": 5,
                        "userid": userid,
                        "phonenumber": user_number,
                        "email": user_email,
                        'tidyspid': tidyspid,
                        "sp_info": tidysp_info,
                        "notification_sent": notification_sent
                    }),
                    Subject='Service Provider Assignment Notification Success'
                )

                record_result['success'] = True
                record_result['notification_sent'] = notification_sent
                record_result['userid'] = userid
                record_result['tidyspid'] = tidyspid

            except Exception as record_error:
                logger.error(f"Failed to process record: {record_error}")

                # Prepare log data with safe variable access
                log_data = {
                    "logtypeid": 4,
                    "categoryid": 36,  # Service Assignment
                    "transactiontypeid": 12,
                    "statusid": 43,
                    "error": str(record_error)
                }

                # Add available context information if defined
                if 'userid' in locals():
                    log_data["userid"] = userid
                if 'tidyspid' in locals():
                    log_data["tidyspid"] = tidyspid
                if 'user_number' in locals():
                    log_data["phonenumber"] = user_number
                if 'user_email' in locals():
                    log_data["email"] = user_email
                if 'tidysp_info' in locals():
                    log_data["sp_info"] = tidysp_info

                # Log error to SNS
                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps(log_data),
                    Subject='Service Provider Assignment Notification Failure'
                )

                record_result['error'] = str(record_error)

            # Add this record's result to the overall results
            results.append(record_result)

        # Return summary of all processed records
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': any(r['success'] for r in results),
                'processed_count': len(results),
                'success_count': sum(1 for r in results if r['success']),
                'results': results
            })
        }

    except Exception as e:
        logger.error(f"Lambda execution error: {e}")

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Internal Server Error",
                "error": str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()