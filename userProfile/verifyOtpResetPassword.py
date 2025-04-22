import json
import boto3
import logging
import hashlib
import os
import re
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, send_email_via_ses, hash_password

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

# Password requirements
PASSWORD_MIN_LENGTH = 8
PASSWORD_REGEX = r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$'
PASSWORD_REQUIREMENTS = [
    "At least 8 characters long",
    "Contains at least one uppercase letter",
    "Contains at least one lowercase letter",
    "Contains at least one number",
    "Contains at least one special character (@$!%*?&)"
]


def validate_password_strength(password):
    """Validate password against security requirements"""
    # Check password length
    if len(password) < PASSWORD_MIN_LENGTH:
        return False, f"Password must be at least {PASSWORD_MIN_LENGTH} characters long"

    # Check password complexity using regex
    if not re.match(PASSWORD_REGEX, password):
        return False, f"Password does not meet complexity requirements: {', '.join(PASSWORD_REQUIREMENTS)}"

    return True, "Password meets all requirements"


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))

        # Extract parameters
        email = body.get('email')
        otp = body.get('otp')
        new_password = body.get('new_password')

        # Validate required parameters
        if not email or not otp or not new_password:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: email, otp, and new_password are required'
                })
            }

        # Validate password strength
        is_valid, message = validate_password_strength(new_password)
        if not is_valid:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': message})
            }

        # Get client IP for security logging
        client_ip = event.get('requestContext', {}).get('identity', {}).get('sourceIp', 'unknown')

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Find user by email
        cursor.execute("""
            SELECT userid
            FROM users
            WHERE email = %s AND isdisabled = FALSE
        """, (email,))

        user = cursor.fetchone()

        if not user:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'User not found or account is disabled'})
            }

        user_id = user['userid']

        # Verify OTP
        cursor.execute("""
            SELECT token, expiresat, isused
            FROM password_reset_tokens
            WHERE userid = %s AND token = %s
            ORDER BY createdat DESC
            LIMIT 1
        """, (user_id, otp))

        token_info = cursor.fetchone()

        if not token_info:
            # Log failed OTP verification attempt
            cursor.execute("""
                INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'PASSWORD_RESET_OTP_INVALID',
                json.dumps({
                    'reason': 'Invalid OTP provided',
                    'ip_address': client_ip
                }),
                client_ip
            ))

            connection.commit()

            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid OTP'})
            }

        # Check if OTP is expired
        current_time = datetime.now()
        expiry_time = token_info['expiresat']

        if current_time > expiry_time:
            # Log expired OTP attempt
            cursor.execute("""
                INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'PASSWORD_RESET_OTP_EXPIRED',
                json.dumps({
                    'reason': 'Expired OTP provided',
                    'ip_address': client_ip
                }),
                client_ip
            ))

            connection.commit()

            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'OTP has expired. Please request a new one.'})
            }

        # Check if OTP is already used
        if token_info['isused']:
            # Log used OTP attempt
            cursor.execute("""
                INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'PASSWORD_RESET_OTP_ALREADY_USED',
                json.dumps({
                    'reason': 'Already used OTP provided',
                    'ip_address': client_ip
                }),
                client_ip
            ))

            connection.commit()

            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'OTP has already been used. Please request a new one.'})
            }

        # Begin transaction
        connection.autocommit = False

        # Mark OTP as used
        cursor.execute("""
            UPDATE password_reset_tokens
            SET isused = TRUE, updatedat = NOW()
            WHERE userid = %s AND token = %s
        """, (user_id, otp))

        # Hash the new password
        new_password_hash, new_salt = hash_password(new_password)

        # Update the password in the database
        cursor.execute("""
            UPDATE users
            SET password = %s, salt = %s, lastpasswordchanged = NOW(), updatedat = NOW()
            WHERE userid = %s
            RETURNING email
        """, (new_password_hash, new_salt, user_id))

        user_info = cursor.fetchone()

        # Get user details for notification
        cursor.execute("""
            SELECT u.email, ud.firstname, ud.lastname
            FROM users u
            LEFT JOIN userdetails ud ON u.userid = ud.userid
            WHERE u.userid = %s
        """, (user_id,))

        user_details = cursor.fetchone()
        user_name = f"{user_details.get('firstname', '')} {user_details.get('lastname', '')}" if user_details else ""

        # Log the password reset in the activity logs
        cursor.execute("""
            INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
            VALUES (%s, %s, %s, %s, NOW())
        """, (
            user_id,
            'PASSWORD_RESET_COMPLETED',
            json.dumps({
                'method': 'OTP',
                'ip_address': client_ip,
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
            'Your password has been reset successfully.',
            False
        ))

        # Commit the transaction
        connection.commit()

        # Send email notification
        if user_details and user_details.get('email'):
            user_email = user_details['email']

            email_subject = "Password Reset Successful"
            email_message = f"""
            <h2>Password Reset Successful</h2>
            <p>Dear {user_name},</p>
            <p>Your password has been reset successfully.</p>
            <p><strong>Details:</strong><br>
            Date & Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
            IP Address: {client_ip}</p>
            <p>If you did not make this change, please contact our support team immediately.</p>
            <p>Thank you for using our service!</p>
            """

            try:
                send_email_via_ses(user_email, email_subject, email_message)
                logger.info(f"Password reset confirmation email sent to user {user_id}")
            except Exception as e:
                logger.error(f"Failed to send password reset confirmation email: {e}")

        # Log success to SNS
        log_to_sns(1, 7, 7, 1, {
            "user_id": user_id,
            "client_ip": client_ip
        }, "Password Reset Completed - Success", user_id)

        logger.info(f"Successfully reset password for user {user_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Password reset successfully',
                'note': 'You can now log in with your new password'
            })
        }

    except Exception as e:
        logger.error(f"Failed to reset password: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 7, 43, {
            "email": email if 'email' in locals() else None,
            "error": str(e)
        }, "Password Reset - Failed", user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to reset password',
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