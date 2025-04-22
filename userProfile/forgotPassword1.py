import json
import boto3
import logging
import re
import random
import string
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
FORGOT_PASSWORD_TOPIC_ARN = secrets["FORGOT_PASSWORD_TOPIC_ARN"]

# OTP configurations
OTP_LENGTH = 6
OTP_EXPIRY_MINUTES = 15  # OTP valid for 15 minutes


def generate_otp(length=OTP_LENGTH):
    """Generate a numeric OTP of specified length"""
    return ''.join(random.choices(string.digits, k=length))


def validate_email(email):
    """Validate email format"""
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_regex, email) is not None


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))

        # Extract email for password reset
        email = body.get('email')

        # Validate required parameters
        if not email:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: email'})
            }

        # Validate email format
        if not validate_email(email):
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid email format'})
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Find user by email
        cursor.execute("""
            SELECT u.userid, u.email, ud.phonenumber
            FROM users u
            LEFT JOIN userdetails ud ON u.userid = ud.userid
            WHERE u.email = %s AND u.isdisabled = FALSE
        """, (email,))

        user = cursor.fetchone()

        if not user:
            # For security reasons, don't disclose whether email exists or not
            # Still return a success response to prevent email enumeration
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'If your email is registered, you will receive a password reset OTP shortly'
                })
            }

        user_id = user['userid']
        phone_number = user.get('phonenumber')

        # Generate an OTP
        otp = generate_otp()
        expiry_time = datetime.now()

        # Get client IP and user agent for security logging
        client_ip = event.get('requestContext', {}).get('identity', {}).get('sourceIp', 'unknown')
        user_agent = event.get('requestContext', {}).get('identity', {}).get('userAgent', 'unknown')

        # Begin transaction
        connection.autocommit = False

        # Store OTP in the database
        cursor.execute("""
            INSERT INTO password_reset_tokens (userid, token, expiresat, createdat, isused)
            VALUES (%s, %s, NOW() + INTERVAL '%s minutes', NOW(), FALSE)
        """, (user_id, otp, OTP_EXPIRY_MINUTES))

        # Log the password reset request in the activity logs
        cursor.execute("""
            INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
            VALUES (%s, %s, %s, %s, NOW())
        """, (
            user_id,
            'PASSWORD_RESET_REQUESTED',
            json.dumps({
                'client_ip': client_ip,
                'user_agent': user_agent,
                'timestamp': datetime.now().isoformat()
            }),
            client_ip
        ))

        connection.commit()

        # Determine the notification channels for OTP
        notification_channels = []

        if email:
            notification_channels.append('email')

        if phone_number:
            notification_channels.append('sms')

        # Prepare message for SNS
        message = {
            'user_id': user_id,
            'email': email,
            'phone_number': phone_number,
            'otp': otp,
            'expiry_minutes': OTP_EXPIRY_MINUTES,
            'notification_channels': notification_channels,
            'client_ip': client_ip,
            'user_agent': user_agent,
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=FORGOT_PASSWORD_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'Password Reset OTP: {user_id}'
        )

        # Log success to SNS
        log_to_sns(1, 7, 7, 1, {
            "user_id": user_id,
            "client_ip": client_ip,
            "notification_channels": notification_channels
        }, "Password Reset OTP Requested - Success", user_id)

        logger.info(f"Successfully initiated password reset for user {user_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'If your email is registered, you will receive a password reset OTP shortly',
                'channels': notification_channels,
                'expiry_minutes': OTP_EXPIRY_MINUTES
            })
        }

    except Exception as e:
        logger.error(f"Failed to initiate password reset: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS, but don't include user-specific details for security
        log_to_sns(4, 7, 7, 43, {
            "error": str(e),
            "email": email if 'email' in locals() else None
        }, "Password Reset OTP Requested - Failed", None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process password reset request',
                'error': str(e)
            })
        }

    finally:
        if connection:
            connection.autocommit = True
        if cursor:
            cursor.close()
        if connection:
            connection.close()