import json
import boto3
import logging
import pyotp
import qrcode
import io
import base64
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, get_client_ip, get_user_agent

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
TWO_FA_TOPIC_ARN = secrets["TWO_FA_TOPIC_ARN"]


def generate_qr_code(secret_key, user_email):
    """Generate QR code for 2FA setup"""
    # Create the provisioning URI
    totp = pyotp.TOTP(secret_key)
    uri = totp.provisioning_uri(name=user_email, issuer_name="TidyZon")

    # Generate QR code
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(uri)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white")

    # Convert to base64
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)

    img_str = base64.b64encode(buffer.getvalue()).decode()
    return f"data:image/png;base64,{img_str}"


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")

        body = json.loads(event.get('body', '{}'))
        action = body.get('action')  # 'enable' or 'disable'

        # Additional information for context
        client_ip = get_client_ip(event)
        user_agent = get_user_agent(event)

        # Validate required parameters
        if not user_id or not action:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: userid and action are required'
                })
            }

        # Validate action type
        if action not in ['enable', 'disable', 'verify']:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Invalid action. Must be one of: enable, disable, verify'
                })
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Get user information
        cursor.execute("""
            SELECT u.userid, u.email, u.is2faenabled, u.twofa_secret
            FROM users u
            WHERE u.userid = %s
        """, (user_id,))

        user = cursor.fetchone()

        if not user:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'User not found'})
            }

        # Process based on action
        if action == 'enable':
            # If 2FA is already enabled, return message
            if user['is2faenabled']:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': '2FA is already enabled for this user'})
                }

            # Generate new secret key if not already present
            secret_key = user.get('twofa_secret')
            if not secret_key:
                secret_key = pyotp.random_base32()

                # Store secret key in database
                cursor.execute("""
                    UPDATE users
                    SET twofa_secret = %s, updatedat = NOW()
                    WHERE userid = %s
                """, (secret_key, user_id))

                connection.commit()

            # Generate QR code for the frontend
            qr_code = generate_qr_code(secret_key, user['email'])

            # Prepare response with QR code
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Ready for 2FA setup. Please scan the QR code with your authenticator app.',
                    'qr_code': qr_code,
                    'secret_key': secret_key,
                    'next_step': 'verify'
                })
            }

        elif action == 'verify':
            # Verify the provided OTP code
            otp_code = body.get('otp_code')
            if not otp_code:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Missing OTP code'})
                }

            # Get the secret key
            secret_key = user.get('twofa_secret')
            if not secret_key:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': '2FA setup not initiated. Please enable 2FA first.'})
                }

            # Verify the OTP
            totp = pyotp.TOTP(secret_key)
            is_valid = totp.verify(otp_code)

            if not is_valid:
                # Log failed verification attempt
                cursor.execute("""
                    INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, user_agent, createdat)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    '2FA_VERIFICATION_FAILED',
                    json.dumps({
                        'action': 'verify',
                        'client_ip': client_ip,
                        'timestamp': datetime.now().isoformat()
                    }),
                    client_ip,
                    user_agent
                ))

                connection.commit()

                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Invalid OTP code. Please try again.'})
                }

            # OTP is valid, prepare message for SNS
            message = {
                'user_id': user_id,
                'action': 'enable',  # Enable 2FA after successful verification
                'email': user['email'],
                'secret_key': secret_key,
                'client_ip': client_ip,
                'user_agent': user_agent,
                'timestamp': datetime.now().isoformat()
            }

            # Publish to SNS for asynchronous processing
            sns_client.publish(
                TopicArn=TWO_FA_TOPIC_ARN,
                Message=json.dumps(message),
                Subject=f'2FA Enable: {user_id}'
            )

            # Log success to SNS
            log_to_sns(1, 7, 12, 1, {
                "user_id": user_id,
                "action": 'verify'
            }, "2FA Verification - Success", user_id)

            return {
                'statusCode': 202,  # Accepted
                'body': json.dumps({
                    'message': 'OTP verified successfully. 2FA will be enabled shortly.',
                    'verified': True
                })
            }

        elif action == 'disable':
            # If 2FA is not enabled, return message
            if not user['is2faenabled']:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': '2FA is not enabled for this user'})
                }

            # Verify the provided OTP code if required for disabling
            require_otp_for_disable = body.get('require_otp', True)

            if require_otp_for_disable:
                otp_code = body.get('otp_code')
                if not otp_code:
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'message': 'OTP code is required to disable 2FA'})
                    }

                # Get the secret key
                secret_key = user.get('twofa_secret')
                if not secret_key:
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'message': 'No 2FA secret key found'})
                    }

                # Verify the OTP
                totp = pyotp.TOTP(secret_key)
                is_valid = totp.verify(otp_code)

                if not is_valid:
                    # Log failed verification attempt
                    cursor.execute("""
                        INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, user_agent, createdat)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                    """, (
                        user_id,
                        '2FA_VERIFICATION_FAILED',
                        json.dumps({
                            'action': 'disable',
                            'client_ip': client_ip,
                            'timestamp': datetime.now().isoformat()
                        }),
                        client_ip,
                        user_agent
                    ))

                    connection.commit()

                    return {
                        'statusCode': 400,
                        'body': json.dumps({'message': 'Invalid OTP code. Please try again.'})
                    }

            # OTP is valid or not required, prepare message for SNS
            message = {
                'user_id': user_id,
                'action': 'disable',
                'email': user['email'],
                'client_ip': client_ip,
                'user_agent': user_agent,
                'timestamp': datetime.now().isoformat()
            }

            # Publish to SNS for asynchronous processing
            sns_client.publish(
                TopicArn=TWO_FA_TOPIC_ARN,
                Message=json.dumps(message),
                Subject=f'2FA Disable: {user_id}'
            )

            # Log success to SNS
            log_to_sns(1, 7, 12, 1, {
                "user_id": user_id,
                "action": 'disable'
            }, "2FA Disable - Initiated", user_id)

            return {
                'statusCode': 202,  # Accepted
                'body': json.dumps({
                    'message': '2FA will be disabled shortly.',
                    'status': 'processing'
                })
            }

    except Exception as e:
        logger.error(f"Failed to process 2FA request: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 12, 43, {
            "user_id": user_id,
            "action": action if 'action' in locals() else None,
            "error": str(e)
        }, "2FA Management - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process 2FA request',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()