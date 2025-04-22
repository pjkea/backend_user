import json
import boto3
import logging
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


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Process SNS records
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            # Extract data from message
            user_id = message.get('user_id')
            activity_type = message.get('activity_type')
            details = message.get('details', {})
            client_ip = message.get('client_ip', 'unknown')
            user_agent = message.get('user_agent', 'unknown')

            # Connect to database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Record activity in the database
            cursor.execute("""
                INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, user_agent, createdat)
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING logid
            """, (
                user_id,
                activity_type,
                json.dumps(details),
                client_ip,
                user_agent
            ))

            log_result = cursor.fetchone()
            log_id = log_result['logid']

            # Implement security monitoring based on activity type
            if activity_type == 'LOGIN_FAILED':
                # Check for suspicious login attempts
                cursor.execute("""
                    SELECT COUNT(*) as failed_attempts
                    FROM user_activity_logs
                    WHERE userid = %s AND activity_type = 'LOGIN_FAILED'
                    AND createdat > NOW() - INTERVAL '30 minutes'
                """, (user_id,))

                failed_attempts = cursor.fetchone()['failed_attempts']

                # If there are multiple failed login attempts in a short period
                if failed_attempts >= 5:
                    # Flag the account for suspicious activity
                    cursor.execute("""
                        INSERT INTO security_alerts (userid, alert_type, details, createdat)
                        VALUES (%s, %s, %s, NOW())
                    """, (
                        user_id,
                        'SUSPICIOUS_LOGIN_ATTEMPTS',
                        json.dumps({
                            'failed_attempts': failed_attempts,
                            'time_window': '30 minutes',
                            'latest_ip': client_ip,
                            'latest_user_agent': user_agent
                        })
                    ))

                    # Create notification for admin
                    cursor.execute("""
                        INSERT INTO admin_notifications (alert_type, message, details, isread, createdat)
                        VALUES (%s, %s, %s, %s, NOW())
                    """, (
                        'SUSPICIOUS_LOGIN_ATTEMPTS',
                        f"User {user_id} has had {failed_attempts} failed login attempts in the last 30 minutes",
                        json.dumps({
                            'user_id': user_id,
                            'failed_attempts': failed_attempts,
                            'time_window': '30 minutes',
                            'latest_ip': client_ip,
                            'latest_user_agent': user_agent
                        }),
                        False
                    ))

            elif activity_type == 'PASSWORD_CHANGED':
                # Create notification for the user about password change
                cursor.execute("""
                    INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    'SECURITY',
                    'Your password was changed. If you did not make this change, please contact support immediately.',
                    False
                ))

            elif activity_type in ['PROFILE_UPDATE', 'PAYMENT_METHOD_ADDED', 'PAYMENT_METHOD_REMOVED']:
                # Create notification for the user about important account changes
                cursor.execute("""
                    INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    'ACCOUNT',
                    f'Your account information was updated: {activity_type.replace("_", " ").title()}',
                    False
                ))

            # Commit the changes
            connection.commit()

            # Log success to SNS
            log_to_sns(1, 7, 11, 1, {
                "user_id": user_id,
                "activity_type": activity_type,
                "log_id": log_id
            }, "User Activity Recorded - Success", user_id)

            logger.info(f"Successfully recorded user activity {activity_type} for user {user_id}")

        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'User activity recorded successfully',
                'log_id': log_id if 'log_id' in locals() else None
            })
        }

    except Exception as e:
        logger.error(f"Failed to record user activity: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 11, 43, {
            "user_id": user_id if 'user_id' in locals() else None,
            "activity_type": activity_type if 'activity_type' in locals() else None,
            "error": str(e)
        }, "User Activity Recording - Failed", user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to record user activity',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()