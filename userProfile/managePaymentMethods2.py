import json
import boto3
import logging
import stripe
import hashlib
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, send_email_via_ses

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

# Stripe API Key
STRIPE_API_KEY = secrets.get("STRIPE_API_KEY")
stripe.api_key = STRIPE_API_KEY


def mask_card_number(card_number):
    """Mask a card number for display, showing only the last 4 digits"""
    return f"**** **** **** {card_number[-4:]}" if card_number else None


def generate_payment_method_fingerprint(payment_method):
    """Generate a unique fingerprint for a payment method to avoid duplicates"""
    method_type = payment_method.get('type', '').lower()
    fingerprint_data = ""

    if method_type in ['credit_card', 'debit_card']:
        card_number = payment_method.get('card_number', '')
        # Use only last 4 digits + card type for fingerprint
        fingerprint_data = f"{method_type}:{card_number[-4:]}"
    elif method_type == 'bank_account':
        account_number = payment_method.get('account_number', '')
        routing_number = payment_method.get('routing_number', '')
        # Use account and routing info for fingerprint
        fingerprint_data = f"{method_type}:{routing_number}:{account_number[-4:]}"

    # Hash the fingerprint data
    return hashlib.md5(fingerprint_data.encode()).hexdigest()


def store_payment_method_in_stripe(payment_method, customer_id=None):
    """Store payment method in Stripe and return token"""
    method_type = payment_method.get('type', '').lower()

    try:
        # Create Stripe customer if not provided
        if not customer_id:
            customer = stripe.Customer.create(
                name=payment_method.get('card_holder_name') or payment_method.get('account_holder_name'),
                metadata={'source': 'tidyzon'}
            )
            customer_id = customer.id

        # Create payment method based on type
        if method_type in ['credit_card', 'debit_card']:
            # Create card token
            token = stripe.Token.create(
                card={
                    'number': payment_method.get('card_number'),
                    'exp_month': int(payment_method.get('expiration_date', '12/99').split('/')[0]),
                    'exp_year': int(f"20{payment_method.get('expiration_date', '12/99').split('/')[1]}"),
                    'cvc': payment_method.get('cvv'),
                    'name': payment_method.get('card_holder_name')
                }
            )

            # Attach card to customer
            payment_source = stripe.Customer.create_source(
                customer_id,
                source=token.id
            )

            return {
                'customer_id': customer_id,
                'payment_token': token.id,
                'payment_source_id': payment_source.id,
                'last4': payment_source.last4,
                'brand': payment_source.brand,
                'exp_month': payment_source.exp_month,
                'exp_year': payment_source.exp_year
            }

        elif method_type == 'bank_account':
            # Create bank account token
            token = stripe.Token.create(
                bank_account={
                    'country': 'US',
                    'currency': 'usd',
                    'account_holder_name': payment_method.get('account_holder_name'),
                    'account_holder_type': 'individual',
                    'routing_number': payment_method.get('routing_number'),
                    'account_number': payment_method.get('account_number')
                }
            )

            # Attach bank account to customer
            payment_source = stripe.Customer.create_source(
                customer_id,
                source=token.id
            )

            return {
                'customer_id': customer_id,
                'payment_token': token.id,
                'payment_source_id': payment_source.id,
                'last4': payment_source.last4,
                'bank_name': payment_source.bank_name,
                'routing_number': payment_source.routing_number
            }

        else:
            raise ValueError(f"Unsupported payment method type: {method_type}")

    except stripe.error.StripeError as e:
        logger.error(f"Stripe error: {e}")
        raise


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Process SNS records
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            # Extract data from message
            user_id = message.get('user_id')
            action = message.get('action', 'add')
            payment_method = message.get('payment_method', {})
            payment_method_id = message.get('payment_method_id')
            client_ip = message.get('client_ip', 'unknown')

            # Connect to database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Get user details for notification
            cursor.execute("""
                SELECT u.email, ud.firstname, ud.lastname
                FROM users u
                LEFT JOIN userdetails ud ON u.userid = ud.userid
                WHERE u.userid = %s
            """, (user_id,))

            user_details = cursor.fetchone()
            user_email = user_details.get('email') if user_details else None
            user_name = f"{user_details.get('firstname', '')} {user_details.get('lastname', '')}" if user_details else ""

            # Begin transaction
            connection.autocommit = False

            # Process based on action
            if action == "add":
                method_type = payment_method.get('type', '').lower()

                # Check if user already has a Stripe customer ID
                cursor.execute("""
                    SELECT stripecustomerid
                    FROM users
                    WHERE userid = %s
                """, (user_id,))

                user_info = cursor.fetchone()
                stripe_customer_id = user_info.get('stripecustomerid') if user_info else None

                # Store payment method in Stripe
                stripe_result = store_payment_method_in_stripe(payment_method, stripe_customer_id)

                # Update user with Stripe customer ID if not already set
                if not stripe_customer_id:
                    cursor.execute("""
                        UPDATE users
                        SET stripecustomerid = %s, updatedat = NOW()
                        WHERE userid = %s
                    """, (stripe_result['customer_id'], user_id))

                # Generate a fingerprint for this payment method
                fingerprint = generate_payment_method_fingerprint(payment_method)

                # Insert payment method into database
                cursor.execute("""
                    INSERT INTO userpaymentsources (
                        userid, paymenttype, cardlast4, expirationdate, cardholdername, 
                        isdefault, stripepaymentid, fingerprint, createdat
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    RETURNING paymentsourceid
                """, (
                    user_id,
                    method_type,
                    stripe_result.get('last4', payment_method.get('card_number', '')[-4:] if payment_method.get(
                        'card_number') else None),
                    payment_method.get('expiration_date'),
                    payment_method.get('card_holder_name') or payment_method.get('account_holder_name'),
                    payment_method.get('is_default', False),
                    stripe_result.get('payment_source_id'),
                    fingerprint
                ))

                payment_source = cursor.fetchone()
                new_payment_method_id = payment_source['paymentsourceid']

                # If this is the default payment method, update other methods
                if payment_method.get('is_default', False):
                    cursor.execute("""
                        UPDATE userpaymentsources
                        SET isdefault = FALSE
                        WHERE userid = %s AND paymentsourceid != %s
                    """, (user_id, new_payment_method_id))

                # Log the payment method addition to activity logs
                cursor.execute("""
                    INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    'PAYMENT_METHOD_ADDED',
                    json.dumps({
                        'payment_method_id': new_payment_method_id,
                        'type': method_type,
                        'last4': stripe_result.get('last4'),
                        'is_default': payment_method.get('is_default', False)
                    }),
                    client_ip
                ))

                # Send notification to user
                payment_method_details = f"{method_type.replace('_', ' ').title()} ending in {stripe_result.get('last4')}"

                result = {
                    'action': 'add',
                    'payment_method_id': new_payment_method_id,
                    'payment_method_details': payment_method_details,
                    'is_default': payment_method.get('is_default', False)
                }

            elif action == "update":
                # Get current payment method details
                cursor.execute("""
                    SELECT paymentsourceid, paymenttype, cardlast4, expirationdate, cardholdername, 
                           isdefault, stripepaymentid, fingerprint
                    FROM userpaymentsources
                    WHERE userid = %s AND paymentsourceid = %s
                """, (user_id, payment_method_id))

                current_payment_method = cursor.fetchone()

                if not current_payment_method:
                    raise ValueError("Payment method not found")

                # Update fields that can be changed
                update_fields = []
                update_values = []

                if payment_method.get('expiration_date'):
                    update_fields.append("expirationdate = %s")
                    update_values.append(payment_method.get('expiration_date'))

                if payment_method.get('card_holder_name') or payment_method.get('account_holder_name'):
                    update_fields.append("cardholdername = %s")
                    update_values.append(
                        payment_method.get('card_holder_name') or payment_method.get('account_holder_name'))

                if 'is_default' in payment_method:
                    update_fields.append("isdefault = %s")
                    update_values.append(payment_method.get('is_default'))

                    # If being set as default, update other methods
                    if payment_method.get('is_default'):
                        cursor.execute("""
                            UPDATE userpaymentsources
                            SET isdefault = FALSE
                            WHERE userid = %s AND paymentsourceid != %s
                        """, (user_id, payment_method_id))

                # Only update if there are changes
                if update_fields:
                    update_fields.append("updatedat = NOW()")
                    query = f"""
                        UPDATE userpaymentsources
                        SET {', '.join(update_fields)}
                        WHERE userid = %s AND paymentsourceid = %s
                    """
                    cursor.execute(query, update_values + [user_id, payment_method_id])

                # Log the payment method update to activity logs
                cursor.execute("""
                    INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    'PAYMENT_METHOD_UPDATED',
                    json.dumps({
                        'payment_method_id': payment_method_id,
                        'type': current_payment_method['paymenttype'],
                        'last4': current_payment_method['cardlast4'],
                        'updated_fields': [field.split(' = ')[0] for field in update_fields if 'updatedat' not in field]
                    }),
                    client_ip
                ))

                # Send notification to user
                payment_method_details = f"{current_payment_method['paymenttype'].replace('_', ' ').title()} ending in {current_payment_method['cardlast4']}"

                result = {
                    'action': 'update',
                    'payment_method_id': payment_method_id,
                    'payment_method_details': payment_method_details,
                    'updated_fields': [field.split(' = ')[0] for field in update_fields if 'updatedat' not in field]
                }

            elif action == "remove":
                # Get current payment method details
                cursor.execute("""
                    SELECT paymentsourceid, paymenttype, cardlast4, stripepaymentid, isdefault
                    FROM userpaymentsources
                    WHERE userid = %s AND paymentsourceid = %s
                """, (user_id, payment_method_id))

                current_payment_method = cursor.fetchone()

                if not current_payment_method:
                    raise ValueError("Payment method not found")

                # Get Stripe customer ID
                cursor.execute("""
                    SELECT stripecustomerid
                    FROM users
                    WHERE userid = %s
                """, (user_id,))

                user_info = cursor.fetchone()
                stripe_customer_id = user_info.get('stripecustomerid')

                # Delete payment method from Stripe
                if stripe_customer_id and current_payment_method['stripepaymentid']:
                    try:
                        stripe.Customer.delete_source(
                            stripe_customer_id,
                            current_payment_method['stripepaymentid']
                        )
                    except stripe.error.StripeError as e:
                        logger.warning(f"Failed to delete payment method from Stripe: {e}")

                # Delete payment method from database
                cursor.execute("""
                    DELETE FROM userpaymentsources
                    WHERE userid = %s AND paymentsourceid = %s
                """, (user_id, payment_method_id))

                # If this was the default payment method, set a new default if available
                if current_payment_method['isdefault']:
                    cursor.execute("""
                        UPDATE userpaymentsources
                        SET isdefault = TRUE
                        WHERE userid = %s
                        ORDER BY createdat DESC
                        LIMIT 1
                    """, (user_id,))

                # Log the payment method removal to activity logs
                cursor.execute("""
                    INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (
                    user_id,
                    'PAYMENT_METHOD_REMOVED',
                    json.dumps({
                        'payment_method_id': payment_method_id,
                        'type': current_payment_method['paymenttype'],
                        'last4': current_payment_method['cardlast4']
                    }),
                    client_ip
                ))

                # Send notification to user
                payment_method_details = f"{current_payment_method['paymenttype'].replace('_', ' ').title()} ending in {current_payment_method['cardlast4']}"

                result = {
                    'action': 'remove',
                    'payment_method_id': payment_method_id,
                    'payment_method_details': payment_method_details
                }

            # Create notification in app
            notification_messages = {
                'add': f"Payment method added: {result.get('payment_method_details')}",
                'update': f"Payment method updated: {result.get('payment_method_details')}",
                'remove': f"Payment method removed: {result.get('payment_method_details')}"
            }

            cursor.execute("""
                INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'PAYMENT_METHOD',
                notification_messages.get(action, f"Payment method {action}ed"),
                False
            ))

            # Send email notification
            if user_email:
                email_subjects = {
                    'add': "Payment Method Added",
                    'update': "Payment Method Updated",
                    'remove': "Payment Method Removed"
                }

                email_subject = email_subjects.get(action, f"Payment Method {action.capitalize()}ed")

                email_message = f"""
                <h2>{email_subject}</h2>
                <p>Dear {user_name},</p>
                <p>{notification_messages.get(action, f"Your payment method has been {action}ed.")}</p>
                <p><strong>Details:</strong><br>
                Date & Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>If you did not make this change, please contact our support team immediately.</p>
                <p>Thank you for using our service!</p>
                """

                try:
                    send_email_via_ses(user_email, email_subject, email_message)
                    logger.info(f"Payment method {action} notification email sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send payment method notification email: {e}")

            # Commit the transaction
            connection.commit()

            # Log success to SNS
            log_to_sns(1, 7, 8, 1, {
                "user_id": user_id,
                "action": action,
                "payment_method_id": payment_method_id if action in ["update", "remove"] else result.get(
                    'payment_method_id')
            }, f"Payment Method {action.capitalize()} - Success", user_id)

            logger.info(f"Successfully processed payment method {action} for user {user_id}")

            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Payment method {action}ed successfully',
                    'result': result
                })
            }

    except Exception as e:
        logger.error(f"Failed to process payment method {action}: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 8, 43, {
            "user_id": user_id if 'user_id' in locals() else 'unknown',
            "action": action if 'action' in locals() else None,
            "error": str(e)
        }, f"Payment Method {action.capitalize() if 'action' in locals() else 'Request'} - Failed",
                   user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Failed to process payment method {action if "action" in locals() else "request"}',
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