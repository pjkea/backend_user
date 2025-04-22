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
            action = message.get('action')  # 'enable' or 'disable'
            email = message.get('email')
            secret_key = message.get('secret_key')
            client_ip = message.get('client_ip', 'unknown')
            user_agent = message.get('user_agent', 'unknown')

            # Connect to database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Get user contact information
            cursor.execute("""
                SELECT u.email, ud.phonenumber, ud.firstname, ud.lastname
                FROM users u
                JOIN userdetails ud ON u.userid = ud.userid
                WHERE u.userid = %s
            """, (user_id,))

            user_info = cursor.fetchone()

            if not user_info:
                logger.warning(f"User information not found for user {user_id}")
                continue

            user_name = f"{user_info.get('firstname', '')} {user_info.get('lastname', '')}"
            phone_number = user_info.get('phonenumber')

            # Begin transaction
            connection.autocommit = False

            if action == 'enable':
                # Enable 2FA for the user
                cursor.execute("""
                    UPDATE users
                    SET is2faenabled = TRUE, twofa_secret = %s, updatedat = NOW()
                    WHERE userid = %s
                """, (secret_key, user_id))

                # Record the 2FA enablement in activity logs
                cursor.execute("""
                    INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, user_agent, createdat)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    '2FA_ENABLED',
                    json.dumps({
                        'timestamp': datetime.now().isoformat(),
                        'client_ip': client_ip
                    }),
                    client_ip,
                    user_agent
                ))

                # Create security notification for the user
                cursor.execute("""
                    INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    'SECURITY',
                    'Two-factor authentication has been enabled for your account.',
                    False
                ))

                # Send email notification
                if email:
                    email_subject = "Two-Factor Authentication Enabled"
                    email_message = f"""
                    <h2>Two-Factor Authentication Enabled</h2>
                    <p>Dear {user_name},</p>
                    <p>Two-factor authentication has been successfully enabled for your account.</p>
                    <p>From now on, you will need to enter a verification code from your authenticator app when logging in.</p>
                    <p><strong>Details:</strong><br>
                    Date & Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                    IP Address: {client_ip}</p>
                    <p>If you did not make this change, please contact our support team immediately.</p>
                    <p>Thank you for enhancing the security of your account!</p>
                    """

                    try:
                        send_email_via_ses(email, email_subject, email_message)
                        logger.info(f"2FA enablement email sent to user {user_id}")
                    except Exception as e:
                        logger.error(f"Failed to send 2FA enablement email: {e}")

                # Send SMS notification if phone number exists
                if phone_number:
                    sms_message = "Two-factor authentication has been enabled for your account. If you did not make this change, please contact support immediately."

                    try:
                        send_sms_via_twilio(phone_number, sms_message)
                        logger.info(f"2FA enablement SMS sent to user {user_id}")
                    except Exception as e:
                        logger.error(f"Failed to send 2FA enablement SMS: {e}")

                # Commit the transaction
                connection.commit()

                # Log success to SNS
                log_to_sns(1, 7, 12, 1, {
                    "user_id": user_id,
                    "action": "enable",
                    "status": "completed"
                }, "2FA Enable - Success", user_id)

                logger.info(f"Successfully enabled 2FA for user {user_id}")

            elif action == 'disable':
                # Disable 2FA for the user
                cursor.execute("""
                    UPDATE users
                    SET is2faenabled = FALSE, updatedat = NOW()
                    WHERE userid = %s
                """, (user_id,))

                # Record the 2FA disablement in activity logs
                cursor.execute("""
                    INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, user_agent, createdat)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    '2FA_DISABLED',
                    json.dumps({
                        'timestamp': datetime.now().isoformat(),
                        'client_ip': client_ip
                    }),
                    client_ip,
                    user_agent
                ))

                # Create security notification for the user
                cursor.execute("""
                    INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    'SECURITY',
                    'Two-factor authentication has been disabled for your account.',
                    False
                ))

                # Send email notification
                if email:
                    email_subject = "Two-Factor Authentication Disabled"
                    email_message = f"""
                    <h2>Two-Factor Authentication Disabled</h2>
                    <p>Dear {user_name},</p>
                    <p>Two-factor authentication has been disabled for your account.</p>
                    <p>Your account is now protected only by your password.</p>
                    <p><strong>Details:</strong><br>
                    Date & Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                    IP Address: {client_ip}</p>
                    <p>If you did not make this change, please contact our support team immediately and change your password.</p>
                    <p>For enhanced account security, we recommend enabling two-factor authentication.</p>
                    """

                    try:
                        send_email_via_ses(email, email_subject, email_message)
                        logger.info(f"2FA disablement email sent to user {user_id}")
                    except Exception as e:
                        logger.error(f"Failed to send 2FA disablement email: {e}")

                # Send SMS notification if phone number exists
                if phone_number:
                    sms_message = "Two-factor authentication has been disabled for your account. If you did not make this change, please contact support immediately."

                    try:
                        send_sms_via_twilio(phone_number, sms_message)
                        logger.info(f"2FA disablement SMS sent to user {user_id}")
                    except Exception as e:
                        logger.error(f"Failed to send 2FA disablement SMS: {e}")

                # Commit the transaction
                connection.commit()

                # Log success to SNS
                log_to_sns(1, 7, 12, 1, {
                    "user_id": user_id,
                    "action": "disable",
                    "status": "completed"
                }, "2FA Disable - Success", user_id)

                logger.info(f"Successfully disabled 2FA for user {user_id}")

            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'2FA {action}d successfully',
                    'user_id': user_id,
                    'status': 'completed'
                })
            }

    except Exception as e:
        logger.error(f"Failed to process 2FA {action} request: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 12, 43, {
            "user_id": user_id if 'user_id' in locals() else 'unknown',
            "action": action if 'action' in locals() else None,
            "error": str(e)
        }, f"2FA {action.capitalize() if 'action' in locals() else 'Request'} - Failed",
                   user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Failed to process 2FA {action if "action" in locals() else "request"}',
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