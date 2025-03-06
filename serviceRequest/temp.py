import json
import boto3
import logging
import psycopg2
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection
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

# SNS Topic ARNs
SNS_LOGGING_TOPIC_ARN = secrets.get("SNS_LOGGING_TOPIC_ARN")

# Twilio credentials
TWILIO_ACCOUNT_SID = secrets.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = secrets.get("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = secrets.get("TWILIO_PHONE_NUMBER")

# Initialize Twilio Client
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def send_sms_via_twilio(phone_number, message):
    """Sends an SMS using Twilio"""
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
    """Sends an email using AWS SES"""
    try:
        # Configurable sender email
        sender_email = "notifications@yourdomain.com"

        # Create HTML message with proper formatting
        html_message = message.replace('\n', '<br>')

        # Send email
        ses_client.send_email(
            Source=sender_email,
            Destination={"ToAddresses": [email]},
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Text": {"Data": message},
                    "Html": {"Data": f"<p>{html_message}</p>"}
                }
            }
        )

        logger.info(f"Email sent to {email}")
        return "Email Sent Successfully"
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise


def format_notification_for_provider(message_data):
    """Format notification message for service provider"""
    modifications = message_data.get('modifications', {})
    original = message_data.get('original_values', {})

    # Get user details for notification
    request_id = message_data.get('request_id')

    # Build notification message
    subject = f"Service Request Modification - Request #{request_id}"

    body = f"A client has requested modifications to service request #{request_id}.\n\n"
    body += "Requested changes:\n"

    if 'date' in modifications:
        old_date = original.get('date', 'N/A')
        new_date = modifications.get('date', 'N/A')
        body += f"- Date: {old_date} → {new_date}\n"

    if 'time' in modifications:
        old_time = original.get('time', 'N/A')
        new_time = modifications.get('time', 'N/A')
        body += f"- Time: {old_time} → {new_time}\n"

    if 'add_ons' in modifications:
        body += f"- Service add-ons have been modified\n"

    if 'price' in modifications:
        old_price = original.get('price', 'N/A')
        new_price = modifications.get('price', 'N/A')
        body += f"- Price: ${old_price} → ${new_price}\n"

    body += "\nPlease respond to this request through the TidySP app or portal."
    body += "\n\nNote: If you do not respond within 24 hours, the system will automatically reject the modifications."

    return subject, body


def lambda_handler(event, context):
    connection = None
    cursor = None

    # Process each record
    processed_records = []

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

                # Extract data
                request_id = message.get('request_id')
                userid = message.get('userid')
                tidysp_id = message.get('tidyspid')
                modifications = message.get('modifications', {})
                sp_info = message.get('service_provider', {})

                # Validate required fields
                if not all([request_id, userid, tidysp_id, sp_info]):
                    raise ValueError(f"Missing required fields in message")

                # Format notification for service provider
                subject, body = format_notification_for_provider(message)

                # Get service provider contact info
                sp_email = sp_info.get('email')
                sp_phone = sp_info.get('phone')

                # Track notification methods
                notification_sent = []

                # Send email if available
                if sp_email:
                    send_email_via_ses(sp_email, subject, body)
                    notification_sent.append("email")

                # Send SMS if available
                if sp_phone:
                    # For SMS, send a brief version with a link
                    sms_body = f"New modification requested for service #{request_id}. " \
                               f"Please check your email or log in to respond."
                    send_sms_via_twilio(sp_phone, sms_body)
                    notification_sent.append("sms")

                # Generate confirmation link/token (this would be implementation-specific)
                confirmation_token = f"req-mod-{request_id}-{int(datetime.utcnow().timestamp())}"

                # Store confirmation token in database
                cursor.execute("""
                    INSERT INTO request_modifications 
                    (request_id, userid, tidyspid, modifications, confirmation_token, created_at, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    request_id,
                    userid,
                    tidysp_id,
                    json.dumps(modifications),
                    confirmation_token,
                    datetime.utcnow(),
                    'pending'
                ))

                connection.commit()

                # Log success
                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps({
                        "logtypeid": 1,
                        "categoryid": 2,  # Service Request Modification
                        "transactiontypeid": 14,  # Notification
                        "statusid": 5,  # Success
                        "userid": userid,
                        "request_id": request_id,
                        "tidyspid": tidysp_id,
                        "notification_sent": notification_sent
                    }),
                    Subject=f'Service Provider Notification Success: {request_id}'
                )

                record_result['success'] = True
                record_result['notification_sent'] = notification_sent
                record_result['request_id'] = request_id
                record_result['tidyspid'] = tidysp_id

            except Exception as record_error:
                logger.error(f"Failed to process notification: {record_error}")

                # Log error
                error_data = {
                    "logtypeid": 4,
                    "categoryid": 2,  # Service Request Modification
                    "transactiontypeid": 14,  # Notification
                    "statusid": 43,  # Failed
                    "error": str(record_error)
                }

                # Add context information if available
                if 'request_id' in locals():
                    error_data['request_id'] = request_id
                if 'userid' in locals():
                    error_data['userid'] = userid
                if 'tidysp_id' in locals():
                    error_data['tidyspid'] = tidysp_id

                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps(error_data),
                    Subject='Service Provider Notification Failure'
                )

                record_result['error'] = str(record_error)

            # Add to processed records
            processed_records.append(record_result)

        # Return overall status
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': any(r['success'] for r in processed_records),
                'processed_count': len(processed_records),
                'success_count': sum(1 for r in processed_records if r['success']),
                'results': processed_records
            })
        }

    except Exception as e:
        logger.error(f"Lambda execution error: {e}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'message': f"Failed to process request modifications: {str(e)}"
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()