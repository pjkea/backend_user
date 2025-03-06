import json
import boto3
import logging
from serviceRequest.layers.utils import get_secrets

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
PAYMENT_TOPIC_ARN = secrets['PAYMENT_TOPIC_ARN']


def lambda_handler(event, context):
    try:
        # Parse incoming event data
        body = json.loads(event.get('body', '{}'))

        # Extract payment details
        user_id = body.get('user_id')
        order_id = body.get('order_id')
        amount = body.get('amount')
        payment_source_id = body.get('payment_source_id')

        if not all([user_id, order_id, amount, payment_source_id]):
            raise ValueError('Missing required payment parameters')

        # Prepare payment processing message
        payment_message = {
            'user_id': user_id,
            'order_id': order_id,
            'amount': amount,
            'payment_source_id': payment_source_id,
            'timestamp': context.get_remaining_time_in_millis()
        }

        # Publish message to SNS
        sns_client.publish(
            TopicArn=PAYMENT_TOPIC_ARN,
            Message=json.dumps(payment_message),
            Subject='Payment Request Processing'
        )

        logger.info('Payment Request Processing')

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Payment initiation requested',
            })
        }

    except Exception as e:
        logger.error(f"Payment Processing Failed: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Payment initiation failed',
                'message': str(e)
            })
        }

