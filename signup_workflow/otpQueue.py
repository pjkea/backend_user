import json
import os
import boto3
import logging
from twilio.rest import Client

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
ses_client = boto3.client("ses", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Load secrets from AWS Secrets Manager
secrets = json.loads(secrets_client.get_secret_value(SecretId="tidyzon-env-variables")["SecretString"])

# Twilio Config
TWILIO_ACCOUNT_SID = secrets["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = secrets["TWILIO_AUTH_TOKEN"]
TWILIO_PHONE_NUMBER = "+15017122661"  # Your Twilio number

# SNS Topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
OTP_SNS_TOPIC_ARN = secrets["OTP_SNS_TOPIC_ARN"]

# Email Configuration
SENDER_EMAIL = "no-reply@tidyzon.com"

# Initialize Twilio client
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# CloudWatch Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def send_sms(phone_number, otp_message):
    """Send OTP via Twilio SMS"""
    try:
        message = twilio_client.messages.create(
            body=otp_message,
            from_=TWILIO_PHONE_NUMBER,
            to=phone_number
        )
        logger.info(f"OTP sent via SMS to {phone_number}, Twilio Message SID: {message.sid}")
    except Exception as e:
        logger.error(f"Failed to send OTP via SMS: {str(e)}")
        raise

def send_email(email, otp_message):
    """Send OTP via AWS SES"""
    try:
        ses_client.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [email]},
            Message={
                "Subject": {"Data": "Your OTP Code"},
                "Body": {"Text": {"Data": otp_message}}
            }
        )
        logger.info(f"OTP sent via email to {email}")
    except Exception as e:
        logger.error(f"Failed to send OTP via email: {str(e)}")
        raise

def lambda_handler(event, context):
    """Lambda function to process OTP queue messages"""
    try:
        for record in event['Records']:
            message = json.loads(record["Sns"]["Message"])
            phone_number = message.get("phone_number")
            email = message.get("email")
            otp_code = message.get("otp_code")

            if not otp_code:
                raise ValueError("Missing OTP code")

            otp_message = f"Your OTP code is {otp_code}. It expires in 5 minutes."

            # Send OTP via Twilio SMS if phone number exists
            if phone_number:
                send_sms(phone_number, otp_message)

            # Send OTP via AWS SES if email exists
            if email:
                send_email(email, otp_message)

            # Log success to SNS
            sns_client.publish(
                TopicArn=OTP_SNS_TOPIC_ARN,
                Message=json.dumps({
                    "logtypeid": 2,  # OTP Sent
                    "categoryid": 8,
                    "transactiontypeid": 9,  # Send OTP
                    "statusid": 1,  # Success
                    "phone_number": phone_number,
                    "email": email
                }),
                Subject="SendOTP - Success"
            )

        return {"statusCode": 200, "body": json.dumps({"message": "OTP sent successfully"})}

    except Exception as e:
        logger.error(f"Error processing OTP queue: {str(e)}")

        # Log failure to SNS
        sns_client.publish(
            TopicArn=OTP_SNS_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 3,  # OTP Failed
                "categoryid": 8,
                "transactiontypeid": 9,
                "statusid": 2,  # Failure
                "error": str(e)
            }),
            Subject="SendOTP - Error"
        )

        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

