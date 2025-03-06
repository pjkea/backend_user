import json
import os
import psycopg2
import random
import boto3
import logging
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
from twilio.rest import Client


# Initialize AWS Clients
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")
ses_client = boto3.client("ses", region_name="us-east-1")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Fetch Secrets from AWS Secrets Manager
def get_secrets():
    try:
        response = secrets_manager.get_secret_value(SecretId="tidyzon-env-variables")
        return json.loads(response["SecretString"])
    except Exception as e:
        logger.error(f"Failed to fetch secrets from AWS Secrets Manager: {e}")
        raise

# Load Secrets
secrets = get_secrets()

# Extract Secrets
DB_HOST = secrets["DB_HOST"]
DB_NAME = secrets["DB_NAME"]
DB_USER = secrets["DB_USER"]
DB_PASSWORD = secrets["DB_PASSWORD"]
DB_PORT = secrets.get("DB_PORT", "5432")

SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
OTP_SNS_TOPIC_ARN = secrets["OTP_SNS_TOPIC_ARN"]

TWILIO_ACCOUNT_SID = secrets["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = secrets["TWILIO_AUTH_TOKEN"]
TWILIO_PHONE_NUMBER = secrets["TWILIO_PHONE_NUMBER"] # Replace with your Twilio number

# Initialize Twilio Client
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

def get_db_connection():
    """Establish a secure connection to PostgreSQL and log errors to CloudWatch."""
    try:
        return psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def send_sms_via_twilio(phone_number, otp_code):
    """Sends an OTP SMS using Twilio."""
    try:
        message = twilio_client.messages.create(
            body=f"Your OTP code is {otp_code}. It expires in 5 minutes.",
            from_=TWILIO_PHONE_NUMBER,
            to=phone_number
        )
        return message.sid
    except Exception as e:
        logger.error(f"Failed to send SMS via Twilio: {e}")
        raise


def send_email_via_ses(email, otp_code):
    """Sends an OTP email using AWS SES."""
    try:
        ses_client.send_email(
            Source="no-reply@yourdomain.com",
            Destination={"ToAddresses": [email]},
            Message={
                "Subject": {"Data": "Your OTP Code"},
                "Body": {"Text": {"Data": f"Your OTP code is {otp_code}. It expires in 5 minutes."}}
            }
        )
        return "Email Sent"
    except Exception as e:
        logger.error(f"Failed to send email via SES: {e}")
        raise

def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse input request
        body = json.loads(event.get("body", "{}"))
        email = body.get("email")
        phone_number = body.get("phone_number")
        otp_purpose = body.get("otp_purpose", "signup")  # Default is for signup

        if not email and not phone_number:
            raise ValueError("Missing phone number or email")

        if otp_purpose not in ["signup", "forgot_password"]:
            raise ValueError("Invalid OTP purpose")

        # Generate a 4-digit OTP
        otp_code = random.randint(1000, 9999)
        expiration_time = datetime.utcnow() + timedelta(minutes=5)  # OTP valid for 5 mins

        # Database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Store OTP in the database
        cursor.execute("""
            INSERT INTO public.otpverification (phone_number, email, otp_code, expiration_time, otp_purpose)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (phone_number)
            DO UPDATE
            SET otp_code = EXCLUDED.otp_code, expiration_time = EXCLUDED.expiration_time, otp_purpose = EXCLUDED.otp_purpose
        """, (phone_number, email, otp_code, expiration_time, otp_purpose))

        connection.commit()

        # Send OTP via SMS or Email
        message_status = None
        if phone_number:
            message_status = send_sms_via_twilio(phone_number, otp_code)
        elif email:
            message_status = send_email_via_ses(email, otp_code)

        # Publish OTP details to SNS Queue (`OTPSNS`) for processing
        sns_client.publish(
            TopicArn=OTP_SNS_TOPIC_ARN,
            Message=json.dumps({
                "phone_number": phone_number,
                "email": email,
                "otp_code": otp_code,
                "expiration_time": expiration_time.isoformat(),
                "otp_purpose": otp_purpose
            }),
            Subject="OTP Request Queued"
        )

        # Log success event to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 1,
                "categoryid": 8,  # OTP
                "transactiontypeid": 9,  # Send OTP
                "statusid": 1,  # Success
                "phone_number": phone_number,
                "email": email,
                "otp_purpose": otp_purpose,
                "message_status": message_status
            }),
            Subject="SendOTP - Success"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "OTP sent successfully",
                "otp_purpose": otp_purpose,
                "message_status": message_status
            })
        }

    except Exception as e:
        # Log error to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 3,
                "categoryid": 8,
                "transactiontypeid": 9,
                "statusid": 2,  # Failure
                "error": str(e),
                "phone_number": phone_number,
                "email": email,
                "otp_purpose": otp_purpose
            }),
            Subject="SendOTP - Error"
        )

        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

