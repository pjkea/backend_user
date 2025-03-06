import json
import boto3
import logging
import psycopg2
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, send_sms_via_twilio, send_email_via_ses
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


def format_notification_for_provider(message_data):
    """Format notification message for service provider"""
    modifications = message_data.get('modifications', {})
    original = message_data.get('original_values', {})

    # Get order details for notification
    order_id = message_data.get('order_id')
    request_id = message_data.get('request_id', 'N/A')

    # Build notification message
    subject = f"Service Request Modification - Order #{order_id}"

    body = f"A client has requested modifications to service order #{order_id} (Request #{request_id}).\n\n"
    body += "Requested changes:\n"

    if 'schedule_for' in modifications or 'date' in modifications or 'time' in modifications:
        old_date = original.get('date', 'N/A')
        old_time = original.get('time', 'N/A')

        # Determine the new date/time values
        if 'schedule_for' in modifications:
            schedule_parts = modifications['schedule_for'].split()
            new_date = schedule_parts[0] if len(schedule_parts) > 0 else 'N/A'
            new_time = schedule_parts[1] if len(schedule_parts) > 1 else 'N/A'
        else:
            new_date = modifications.get('date', old_date)
            new_time = modifications.get('time', old_time)

        if new_date != old_date:
            body += f"- Date: {old_date} → {new_date}\n"

        if new_time != old_time:
            body += f"- Time: {old_time} → {new_time}\n"

    if 'add_ons' in modifications:
        body += f"- Service add-ons have been modified\n"

    if 'total_price' in modifications or 'price' in modifications:
        old_price = original.get('price', 'N/A')
        new_price = modifications.get('total_price', modifications.get('price', 'N/A'))
        body += f"- Price: ${old_price} → ${new_price}\n"

    body += "\nPlease respond to this request through the TidySP app or portal."
    body += "\n\nNote: If you do not respond within 24 hours, the system will automatically reject the modifications."

    return subject, body


def lambda_handler(event, context):
    connection = None
    cursor = None
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
                order_id = message.get('order_id')
                request_id = message.get('request_id')
                userid = message.get('userid')
                tidyspid = message.get('tidyspid')
                modifications = message.get('modifications')
                sp_info = message.get('service_provider')

                if not all([order_id, userid, tidyspid, sp_info]):
                    raise ValueError(f"Missing required fields in message")

                subject, body = format_notification_for_provider(message)

                sp_email = sp_info.get('email')
                sp_phone = sp_info.get('phone')

                # Track notifications
                notification_sent = []

                if sp_email:
                    send_email_via_ses(sp_email, subject, body)
                    notification_sent.append('email')

                if sp_phone:
                    sms_body = f"New modification requested for order #{order_id}. " \
                               f"Please check your email or log in to respond."
                    send_sms_via_twilio(sp_phone, sms_body)
                    notification_sent.append('sms')

                # Log success
                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps({
                        "logtypeid": 1,
                        "categoryid": 1,  # Service Request Modification
                        "transactiontypeid": 14,  # Notification
                        "statusid": 27,  # Success
                        "userid": userid,
                        "order_id": order_id,
                        "request_id": request_id,
                        "tidyspid": tidyspid,
                        "notification_sent": notification_sent
                    }),
                    Subject=f'Service Provider Notification Success: {order_id}'
                )

                logger.info(f"Successfully sent notification for order_id: {order_id}")

                record_result['success'] = True
                record_result['notification_sent'] = notification_sent
                record_result['order_id'] = order_id
                record_result['request_id'] = request_id
                record_result['tidyspid'] = tidyspid

            except Exception as record_error:
                logger.error(f"Failed to send notification: {record_error}")

                # Log error
                error_data = {
                    "logtypeid": 4,
                    "categoryid": 1,  # Service Request Modification
                    "transactiontypeid": 14,  # Notification
                    "statusid": 43,  # Failed
                    "error": str(record_error)
                }

                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps(error_data),
                    Subject='Service Provider Notification Error'
                )

                record_result['error'] = str(record_error)

            # Add result to processed records
            processed_records.append(record_result)

        # Fixed indentation - moved outside the loop
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
                'message': f"Failed to process request modification notifications: {str(e)}"
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()