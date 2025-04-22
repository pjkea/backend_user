import json
import boto3
import logging
import re
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
PAYMENT_METHOD_TOPIC_ARN = secrets["PAYMENT_METHOD_TOPIC_ARN"]

# Payment method validation patterns
CARD_NUMBER_REGEX = r'^[0-9]{13,19}$'
EXPIRATION_REGEX = r'^(0[1-9]|1[0-2])\/[0-9]{2}$'
CVV_REGEX = r'^[0-9]{3,4}$'


def validate_payment_method(payment_method):
    """Validate payment method details"""
    method_type = payment_method.get('type', '').lower()

    if method_type == 'credit_card' or method_type == 'debit_card':
        card_number = payment_method.get('card_number', '')
        expiration_date = payment_method.get('expiration_date', '')
        cvv = payment_method.get('cvv', '')
        card_holder_name = payment_method.get('card_holder_name', '')

        # Validate card number format
        if not re.match(CARD_NUMBER_REGEX, card_number):
            return False, "Invalid card number format"

        # Validate expiration date format (MM/YY)
        if not re.match(EXPIRATION_REGEX, expiration_date):
            return False, "Invalid expiration date format. Use MM/YY"

        # Validate CVV format
        if not re.match(CVV_REGEX, cvv):
            return False, "Invalid CVV format"

        # Validate cardholder name presence
        if not card_holder_name:
            return False, "Cardholder name is required"

        return True, "Payment method validated successfully"

    elif method_type == 'bank_account':
        account_number = payment_method.get('account_number', '')
        routing_number = payment_method.get('routing_number', '')
        account_holder_name = payment_method.get('account_holder_name', '')

        # Validate account number
        if not account_number or len(account_number) < 5:
            return False, "Invalid account number"

        # Validate routing number
        if not routing_number or len(routing_number) < 9:
            return False, "Invalid routing number"

        # Validate account holder name
        if not account_holder_name:
            return False, "Account holder name is required"

        return True, "Payment method validated successfully"

    else:
        return False, f"Unsupported payment method type: {method_type}"


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")
        action = query_params.get("action", "add")  # add, update, remove
        http_method = event.get("httpMethod", "POST")

        # Extract action from HTTP method if not specified
        if not action:
            if http_method == "POST":
                action = "add"
            elif http_method == "PUT":
                action = "update"
            elif http_method == "DELETE":
                action = "remove"

        body = json.loads(event.get('body', '{}'))
        payment_method = body.get('payment_method', {})
        payment_method_id = body.get('payment_method_id')

        # Validate required parameters
        if not user_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: userid'})
            }

        # Additional validations based on action
        if action == "add" and not payment_method:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: payment_method'})
            }

        if action in ["update", "remove"] and not payment_method_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: payment_method_id'})
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify user exists
        cursor.execute("""
            SELECT userid FROM users WHERE userid = %s
        """, (user_id,))

        if not cursor.fetchone():
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'User not found'})
            }

        # For add and update actions, validate payment method details
        if action in ["add", "update"] and payment_method:
            is_valid, message = validate_payment_method(payment_method)
            if not is_valid:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': message})
                }

        # For update and remove actions, verify payment method exists and belongs to user
        if action in ["update", "remove"]:
            cursor.execute("""
                SELECT paymentsourceid FROM userpaymentsources
                WHERE userid = %s AND paymentsourceid = %s
            """, (user_id, payment_method_id))

            if not cursor.fetchone():
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'message': 'Payment method not found or does not belong to the user'
                    })
                }

        # Get client IP for security logging
        client_ip = event.get('requestContext', {}).get('identity', {}).get('sourceIp', 'unknown')

        # Prepare message for SNS based on action
        message = {
            'user_id': user_id,
            'action': action,
            'client_ip': client_ip,
            'timestamp': datetime.now().isoformat()
        }

        if action == "add":
            message['payment_method'] = payment_method
        elif action == "update":
            message['payment_method_id'] = payment_method_id
            message['payment_method'] = payment_method
        elif action == "remove":
            message['payment_method_id'] = payment_method_id

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=PAYMENT_METHOD_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'Payment Method {action.capitalize()}: {user_id}'
        )

        # Log success to SNS
        log_to_sns(1, 7, 8, 1, {
            "user_id": user_id,
            "action": action,
            "payment_method_id": payment_method_id if action in ["update", "remove"] else None
        }, f"Payment Method {action.capitalize()} - Initiated", user_id)

        logger.info(f"Successfully initiated payment method {action} for user {user_id}")

        return {
            'statusCode': 202,  # Accepted
            'body': json.dumps({
                'message': f'Payment method {action} request accepted and is being processed',
                'action': action,
                'payment_method_id': payment_method_id if action in ["update", "remove"] else None
            })
        }

    except Exception as e:
        logger.error(f"Failed to process payment method request: {e}")

        # Log error to SNS
        log_to_sns(4, 7, 8, 43, {
            "user_id": user_id,
            "action": action if 'action' in locals() else None,
            "error": str(e)
        }, "Payment Method Request - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process payment method request',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()