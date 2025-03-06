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

