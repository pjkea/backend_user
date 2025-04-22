import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, send_email_via_ses, send_sms_via_twilio

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


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Process SNS records
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            # Extract data from message
            user_id = message.get('user_id')
            email = message.get('email')
            phone_number = message.get('phone_number')
            otp = message.get('otp')
            expiry_minutes = message.get('expiry_minutes', 15)
            notification_channels = message.get('notification_channels', [])
            client_ip = message.get('client_ip', 'unknown')

            # Connect to database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Get user details for notification
            cursor.execute("""
                SELECT u.email, ud.phonenumber, ud.firstname, ud.lastname
                FROM users u
                LEFT JOIN userdetails ud ON u.userid = ud.userid
                WHERE u.userid = %s
            """, (user_id,))

            user_details = cursor.fetchone()

            if not user_details:
                logger.error(f"User details not found for user {user_id}")
                continue

            user_name = f"{user_details.get('firstname', '')} {user_details.get('lastname', '')}"

            # Send OTP via Email if requested
            email_sent = False
            if 'email' in notification_channels and email:
                email_subject = "Password Reset OTP"
                email_message = f"""
                <h2>Password Reset OTP</h2>
                <p>Dear {user_name},</p>
                <p>You have requested to reset your password. Please use the following One-Time Password (OTP) to complete the process:</p>
                <div style="margin: 20px; padding: 15px; background-color: #f0f0f0; border-radius: 5px; text-align: center;">
                    <h3 style="letter-spacing: 5px; font-size: 24px;">{otp}</h3>
                </div>
                <p>This OTP is valid for {expiry_minutes} minutes. If you did not request this password reset, please ignore this email or contact our support team if you have concerns.</p>
                <p>For security reasons, please do not share this OTP with anyone.</p>
                <p>Thank you for using our service!</p>
                """

                try:
                    send_email_via_ses(email, email_subject, email_message)
                    email_sent = True
                    logger.info(f"Password reset OTP email sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send password reset OTP email: {e}")

            # Send OTP via SMS if requested
            sms_sent = False
            if 'sms' in notification_channels and phone_number:
                sms_message = f"Your password reset OTP is: {otp}. This code will expire in {expiry_minutes} minutes. If you didn't request this, please contact support."

                try:
                    send_sms_via_twilio(phone_number, sms_message)
                    sms_sent = True
                    logger.info(f"Password reset OTP SMS sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send password reset OTP SMS: {e}")

            # Update the OTP delivery status in the database
            cursor.execute("""
                UPDATE password_reset_tokens
                SET email_sent = %s, sms_sent = %s, updatedat = NOW()
                WHERE userid = %s AND token = %s AND isused = FALSE
            """, (email_sent, sms_sent, user_id, otp))

            # Log the OTP delivery in the activity logs
            cursor.execute("""
                INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'PASSWORD_RESET_OTP_SENT',
                json.dumps({
                    'email_sent': email_sent,
                    'sms_sent': sms_sent,
                    'channels': notification_channels,
                    'timestamp': datetime.now().isoformat()
                }),
                client_ip
            ))

            connection.commit()

            # Log success to SNS
            log_to_sns(1, 7, 7, 1, {
                "user_id": user_id,
                "email_sent": email_sent,
                "sms_sent": sms_sent
            }, "Password Reset OTP Sent - Success", user_id)

            logger.info(f"Successfully sent password reset OTP for user {user_id}")

            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Password reset OTP sent successfully',
                    'channels': {
                        'email': email_sent,
                        'sms': sms_sent
                    }
                })
            }

    except Exception as e:
        logger.error(f"Failed to send password reset OTP: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 7, 43, {
            "user_id": user_id if 'user_id' in locals() else 'unknown',
            "error": str(e)
        }, "Password Reset OTP Sent - Failed", user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to send password reset OTP',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()