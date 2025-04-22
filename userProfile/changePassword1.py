import json
import boto3
import logging
import hashlib
import re
import os
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, hash_password

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
PASSWORD_CHANGE_TOPIC_ARN = secrets["PASSWORD_CHANGE_TOPIC_ARN"]

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


def verify_password(stored_password_hash, stored_salt, provided_password):
    """Verify a password against its stored hash and salt"""
    # Hash the provided password with the stored salt
    provided_hash, _ = hash_password(provided_password, stored_salt)

    # Compare the hashes
    return provided_hash == stored_password_hash


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
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")

        body = json.loads(event.get('body', '{}'))
        current_password = body.get('current_password')
        new_password = body.get('new_password')

        # Validate required parameters
        if not user_id or not current_password or not new_password:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: userid, current_password, and new_password are required'
                })
            }

        # Validate new password strength
        is_valid, message = validate_password_strength(new_password)
        if not is_valid:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': message})
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Retrieve user's current password and salt
        cursor.execute("""
            SELECT userid, email, password, salt, lastpasswordchanged
            FROM users
            WHERE userid = %s
        """, (user_id,))

        user = cursor.fetchone()

        if not user:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'User not found'})
            }

        # Verify current password
        stored_password_hash = user['password']
        stored_salt = user['salt']

        if not verify_password(stored_password_hash, stored_salt, current_password):
            # Log failed password verification attempt
            cursor.execute("""
                INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'PASSWORD_VERIFICATION_FAILED',
                json.dumps({
                    'reason': 'Current password verification failed during password change attempt'
                }),
                event.get('requestContext', {}).get('identity', {}).get('sourceIp', 'unknown')
            ))

            connection.commit()

            return {
                'statusCode': 401,
                'body': json.dumps({'message': 'Current password is incorrect'})
            }

        # Get client IP and user agent for security logging
        client_ip = event.get('requestContext', {}).get('identity', {}).get('sourceIp', 'unknown')
        user_agent = event.get('requestContext', {}).get('identity', {}).get('userAgent', 'unknown')

        # Prepare message for SNS
        message = {
            'user_id': user_id,
            'email': user['email'],
            'new_password': new_password,  # Will be hashed in Lambda 2
            'client_ip': client_ip,
            'user_agent': user_agent,
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=PASSWORD_CHANGE_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'Password Change Request: {user_id}'
        )

        # Log success to SNS
        log_to_sns(1, 7, 6, 1, {
            "user_id": user_id,
            "client_ip": client_ip
        }, "Password Change Initiated - Success", user_id)

        logger.info(f"Successfully initiated password change for user {user_id}")

        return {
            'statusCode': 202,  # Accepted
            'body': json.dumps({
                'message': 'Password change request accepted and is being processed',
                'note': 'You will receive a confirmation once your password is updated'
            })
        }

    except Exception as e:
        logger.error(f"Failed to initiate password change: {e}")

        # Log error to SNS
        log_to_sns(4, 7, 6, 43, {
            "user_id": user_id,
            "error": str(e)
        }, "Password Change Initiated - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to initiate password change',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()