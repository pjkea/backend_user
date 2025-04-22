import json
import boto3
import logging
import hashlib
import os
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, send_email_via_ses, send_sms_via_twilio, hash_password

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
            new_password = message.get('new_password')
            client_ip = message.get('client_ip', 'unknown')
            user_agent = message.get('user_agent', 'unknown')

            # Connect to database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Hash the new password
            new_password_hash, new_salt = hash_password(new_password)

            # Begin transaction
            connection.autocommit = False

            # Update the password in the database
            cursor.execute("""
                UPDATE users
                SET password = %s, salt = %s, lastpasswordchanged = NOW(), updatedat = NOW()
                WHERE userid = %s
                RETURNING email, lastpasswordchanged
            """, (new_password_hash, new_salt, user_id))

            user_info = cursor.fetchone()

            if not user_info:
                logger.error(f"User {user_id} not found during password update")
                connection.rollback()
                continue

            # Get user details for notification
            cursor.execute("""
                SELECT u.email, ud.phonenumber, ud.firstname, ud.lastname
                FROM users u
                LEFT JOIN userdetails ud ON u.userid = ud.userid
                WHERE u.userid = %s
            """, (user_id,))

            user_details = cursor.fetchone()

            # Log the password change in the activity logs
            cursor.execute("""
                INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'PASSWORD_CHANGED',
                json.dumps({
                    'client_ip': client_ip,
                    'user_agent': user_agent,
                    'timestamp': datetime.now().isoformat()
                }),
                client_ip
            ))

            # Create a notification record
            cursor.execute("""
                INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'SECURITY',
                'Your password has been changed successfully.',
                False
            ))

            # Commit the transaction
            connection.commit()

            # Send email notification
            if user_details and user_details.get('email'):
                user_email = user_details['email']
                user_name = f"{user_details.get('firstname', '')} {user_details.get('lastname', '')}"

                email_subject = "Password Changed Successfully"
                email_message = f"""
                <h2>Password Changed Successfully</h2>
                <p>Dear {user_name},</p>
                <p>Your password has been changed successfully.</p>
                <p><strong>Details:</strong><br>
                Date & Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                IP Address: {client_ip}</p>
                <p>If you did not make this change, please contact our support team immediately or reset your password.</p>
                <p>Thank you for using our service!</p>
                """

                try:
                    send_email_via_ses(user_email, email_subject, email_message)
                    logger.info(f"Password change confirmation email sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send password change email: {e}")

            # Send SMS notification if phone number exists
            if user_details and user_details.get('phonenumber'):
                phone_number = user_details['phonenumber']
                sms_message = f"Your password was changed successfully. If you did not make this change, please contact support immediately."

                try:
                    send_sms_via_twilio(phone_number, sms_message)
                    logger.info(f"Password change confirmation SMS sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send password change SMS: {e}")

            # Log success to SNS
            log_to_sns(1, 7, 6, 1, {
                "user_id": user_id,
                "client_ip": client_ip,
                "status": "completed"
            }, "Password Change - Success", user_id)

            logger.info(f"Successfully changed password for user {user_id}")

            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Password changed successfully',
                    'user_id': user_id
                })
            }

    except Exception as e:
        logger.error(f"Failed to process password change: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 6, 43, {
            "user_id": user_id if 'user_id' in locals() else 'unknown',
            "error": str(e)
        }, "Password Change - Failed", user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process password change',
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