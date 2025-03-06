import json
import os
import psycopg2
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

# Initialize AWS Clients
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

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

def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Extract input parameters
        body = json.loads(event.get("body", "{}"))
        phone_number = body.get("phone_number")
        email = body.get("email")
        otp_code = body.get("otp_code")
        otp_purpose = body.get("otp_purpose", "signup")  # Default to signup

        if not otp_code:
            raise ValueError("Missing OTP code")

        if not phone_number and not email:
            raise ValueError("Either phone number or email must be provided")

        if otp_purpose not in ["signup", "forgot_password"]:
            raise ValueError("Invalid OTP purpose")

        # Database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Define query conditions based on phone or email
        query_condition = "phone_number = %s" if phone_number else "email = %s"
        identifier = phone_number if phone_number else email

        # Fetch OTP from database
        cursor.execute(f"""
            SELECT * FROM public.otpverification 
            WHERE {query_condition} AND otp_code = %s AND otp_purpose = %s
        """, (identifier, otp_code, otp_purpose))
        otp_entry = cursor.fetchone()

        if not otp_entry:
            raise ValueError("Invalid OTP")

        # Check if OTP has expired
        if otp_entry['expiration_time'] < datetime.utcnow():
            raise ValueError("OTP has expired")

        # Mark OTP as used (optional: you can delete it instead)
        cursor.execute(f"""
            DELETE FROM public.otpverification 
            WHERE {query_condition} AND otp_purpose = %s
        """, (identifier, otp_purpose))
        connection.commit()

        # Log success to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 2,  # OTP Verification
                "categoryid": 8,
                "transactiontypeid": 10,  # Verify OTP
                "statusid": 1,  # Success
                "identifier": identifier,
                "otp_purpose": otp_purpose
            }),
            Subject="VerifyOTP - Success"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "OTP verified successfully", "otp_purpose": otp_purpose})
        }

    except Exception as e:
        # Log failure to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 3,
                "categoryid": 8,
                "transactiontypeid": 10,
                "statusid": 2,  # Failure
                "error": str(e),
                "identifier": identifier if 'identifier' in locals() else None,
                "otp_purpose": otp_purpose if 'otp_purpose' in locals() else None
            }),
            Subject="VerifyOTP - Error"
        )

        return {
            "statusCode": 400,
            "body": json.dumps({"error": str(e)})
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
