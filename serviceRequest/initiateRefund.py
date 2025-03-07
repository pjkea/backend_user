import json
import boto3
import logging
import psycopg2
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']
REFUND_TOPIC_ARN = secrets['REFUND_TOPIC_ARN']


def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        orderid = body.get('orderid')
        userid = body.get('userid')

        if not orderid or not userid:
            logger.error("Missing required parameters: orderid and userid")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameters: orderid and userid'})
            }

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        try:
            # Get provider info
            cursor.execute("""SELECT tidyspid FROM orders WHERE orderid = %s""", (orderid,))
            tidysp = cursor.fetchone()

            if not tidysp:
                raise Exception(f'Order {orderid} not found')

            tidyspid = tidysp['tidyspid']

            # Retrieve order details
            cursor.execute("""SELECT o.*, od.* FROM orders o 
            JOIN orderdetails od ON o.orderid = od.orderid
                            WHERE o.orderid = %s""", (orderid,))
            order_details = cursor.fetchone()

            if not order_details:
                raise Exception(f'Order details for {orderid} not found')

            # Get payment info
            cursor.execute("""SELECT p.paymentid, p.amount, p.orderid, o.orderid, up.paymentsourceid
                            FROM payments p
                            JOIN orders o ON p.orderid = o.orderid
                            JOIN userpaymentsources up ON o.userid = up.paymentsourceid
                            WHERE o.userid = %s""", (userid,))
            payment = cursor.fetchone()

            conn.commit()

            payment_id = None
            original_amount = 0
            refund_amount = 0
            refund_status  = 'NO REFUND'

            if payment:
                payment_id = payment['paymentid']
                original_amount = float(payment['amount'])

            # Calculate refund amount based on time to scheduled service
            scheduled_for = order_details.get('scheduledfor')

            if scheduled_for:
                scheduled_date = datetime.fromisoformat(scheduled_for.replace('Z', '+00:00'))
                current_time = datetime.now()

                hours_difference = (scheduled_date - current_time).total_seconds() / 3600

                # Refund policy: Full refund if cancelled more than 24 hours before service
                # Partial refund (50%) if cancelled between 12-24 hours before service
                # No refund if cancelled less than 12 hours before service
                if hours_difference > 24:
                    # Full refund
                    refund_amount = original_amount
                    refund_status = 'FULL_REFUND'

                elif hours_difference > 12:
                    # Partial refund (50%)
                    refund_amount = original_amount * 0.5
                    refund_status = 'PARTIAL_REFUND'

            # Send to SNS
            sns_client.publish(
                TopicArn=REFUND_TOPIC_ARN,
                Message=json.dumps({
                    'orderid': orderid,
                    'userid': userid,
                    'tidyspid': tidyspid,
                    'paymentid': payment_id,
                    'refundamount': refund_amount,
                    'refundstatus': refund_status,
                })
            )

            # Log to SNS
            sns_client.publish(
                TopicArn=SNS_LOGGING_TOPIC_ARN,
                Message=json.dumps({
                    "logtypeid": 1,
                    "categoryid": 9,  # Refund processed
                    "transactiontypeid": 5,  # Order Cancellation
                    "statusid": 8,  # Refund requested
                    'userid': userid,
                    'orderid': orderid,
                })
            )
            logger.info(f'Refund initiated successfully. Status: {refund_status}, Amount: {refund_amount}')

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Refund initiated successfully',
                    'refundStatus': refund_status,
                    'refundAmount': refund_amount
                })
            }

        except Exception as e:
            conn.rollback()
            logger.error(f'Error processing refund for order {orderid}: {str(e)}')

            # Log error
            sns_client.publish(
                TopicArn=SNS_LOGGING_TOPIC_ARN,
                Message=json.dumps({
                    "logtypeid": 4,
                    "categoryid": 9,  # Refund processed
                    "transactiontypeid": 5,  # Order Cancellation
                    "statusid": 43,  # Failure
                    'userid': userid,
                    'orderid': orderid,
                })
            )

            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Error processing refund',
                    'error': str(e)
                })
            }

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f'Error: {e}')

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Server error',
                'error': str(e)
            })
        }
