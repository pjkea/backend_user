import json
import boto3
import logging
import stripe

from serviceRequest.layers.utils import get_secrets, get_db_connection
from psycopg2.extras import RealDictCursor

# Initialize AWS Services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Stripe API Key
STRIPE_API_KEY = secrets['STRIPE_API_KEY']

# SNS Topics
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        for record in event['Records']:
            # Parse SNS message
            message = json.loads(record['Sns']['Message'])

            # Extract payment details
            user_id = message.get('user_id')
            order_id = message.get('order_id')
            amount = message.get('amount')
            payment_source_id = message.get('payment_source_id')

            # Database connection
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Retrieve user payment source
            cursor.execute("""SELECT * FROM userpaymentsources 
                            WHERE userid = %s AND paymentsourceid = %s""", (user_id, payment_source_id))

            userpaymentsource = cursor.fetchone()

            if not userpaymentsource:
                raise ValueError('Payment source not found for user')

            # Convert amount to cents
            amount_cents = int(float(amount) * 100)

            # Process with Stripe
            try:
                payment_intent = stripe.PaymentIntent.create(
                    amount=amount_cents,
                    currency='usd',
                    payment_method=userpaymentsource['Fingerprint'],
                    confirm=True,
                    metadata={
                        'user_id': user_id,
                        'order_id': order_id,
                        'payment_source_id': payment_source_id
                    }
                )

                # Insert payment into table
                cursor.execute("""INSERT INTO payments (orderid, amount, status, paymentgateway, transactionid)
                VALUES (%s, %s, %s, %s, %s) RETURNING paymentid""",
                               (order_id, float(amount), 1, 'Stripe', payment_intent.id))

                # Fetch the new payment ID
                payment_id = cursor.fetchone()[0]

                connection.commit()

                # Log success to SNS
                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps({
                        "logtypeid": 1,
                        "categoryid": 39,  # Payment Initiated
                        "transactiontypeid": 11,
                        "statusid": 23,  # Payment Completed
                        "userid": user_id,
                        "orderid": order_id,
                    })
                )

                logger.info('Payment successfully created')

                return {'statusCode': 200,
                        'body': json.dumps({
                            'message': 'Payment processed successfully', 'payment_id': payment_id,
                            'transaction_id': payment_intent.id
                        })
                        }

            except stripe.error.StripeError as stripe_error:
                logger.error('Error creating payment: %s', stripe_error)

                connection.rollback()

                # Log failure to SNS
                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps({
                        "logtypeid": 2,
                        "categoryid": 28,  # Payment Failed
                        "transactiontypeid": 11,
                        "statusid": 26,  # Payment Failed
                        "userid": user_id,
                        "orderid": order_id,
                    })
                )

                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'message': 'Payment processing failed',
                        'payment_id': payment_id,
                        'error': str(stripe_error)
                    })
                }

    except Exception as error:
        logger.error('Error creating payment: %s', error)
        connection.rollback()

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(error)
            })
        }

    finally:
        if connection:
            connection.close()
        if cursor:
            cursor.close()