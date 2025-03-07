import json
import boto3
import logging
import psycopg2
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

# SNS Topics
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']


def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        order_id = body.get('orderid')
        user_id = body.get('userid')
        cancellation_reason = body.get('cancellation_reason', 'No reason provided')

        if not order_id or not user_id:
            raise ValueError('Missing required parameters: orderid and userid are required')

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)


        try:
            # Validate cancellation request
            cursor.execute("""SELECT o.* FROM orders o WHERE o.orderid = %s AND o.userid = %s""",
                           (order_id, user_id))

            order = cursor.fetchone()

            if not order:
                conn.rollback()
                return {
                    'statusCode': 404,
                    'body': json.dumps({'message': 'Order not found or not authorized'})
                }

            if order['status'] == 'CANCELLED':
                conn.rollback()
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Order is already cancelled'})
                }

            if order['status'] in ['COMPLETED', 'IN_PROGRESS']:
                conn.rollback()
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Cannot cancel an order that is in progress or completed'})
                }

            # Change status to Cancelled
            cursor.execute("""UPDATE orders SET status = %s, updatedat = NOW()
                            WHERE orderid = %s""", ('CANCELLED', order_id))

            # Update ordernotes with cancellation reason
            cursor.execute("""INSERT INTO ordernotes (orderid, note, createdat)
                            VALUES (%s, %s, NOW())""", (order_id, f"Order cancelled. Reason: {cancellation_reason}"))

            conn.commit()

            # Log to SNS
            sns_client.publish(
                TopicArn=SNS_LOGGING_TOPIC_ARN,
                Message=json.dumps({
                    "logtypeid": 1,
                    "categoryid": 30,  # Service Cancellation
                    "transactiontypeid": 5,  # Order Cancellation
                    "statusid": 13,  # Cancelled
                    'userid': user_id,
                    'orderid': order_id,
                })
            )

            logger.info("Service cancelled successfully")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Service cancelled successfully',
                    'userid': user_id,
                    'orderid': order_id,
                    'refundStatus': 'PENDING',
                    'refundNote': 'If eligible, refund will be processed within 3-5 business days'
                })
            }

        except Exception as db_error:
            conn.rollback()
            logger.error(f"Database error: {str(db_error)}")

            # Log to SNS
            sns_client.publish(
                TopicArn=SNS_LOGGING_TOPIC_ARN,
                Message=json.dumps({
                    "logtypeid": 4,
                    "categoryid": 30,  # Service Cancellation
                    "transactiontypeid": 5,  # Order Cancellation
                    "statusid": 43,  # Failure
                    'userid': user_id,
                    'orderid': order_id,
                })
            )

            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error processing cancellation',
                    'error': str(db_error)
                })
            }

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f"Error: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Server error',
                'error': str(e)
            })
        }

