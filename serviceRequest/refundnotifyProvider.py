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

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']


def lambda_handler(event, context):
    results = []
    try:
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])
            order_id = message['orderid']
            userid = message['userid']
            tidyspid = message['tidyspid']
            payment_id = message['paymentid']
            refund_amount = float(message['refundamount'])
            refund_status = message['refundstatus']

            conn = None
            cursor = None
            try:
                conn = get_db_connection()
                cursor = conn.cursor(cursor_factory=RealDictCursor)

                # Process refund
                refund_id = None
                if refund_amount > 0 and payment_id:
                    # Create refund record
                    cursor.execute("""INSERT INTO payments (orderid, amount, status, createdat)
                    VALUES (%s, %s, %s,NOW()) RETURNING paymentid""", (order_id, -refund_amount, refund_status))

                    refund_id = cursor.fetchone()['paymentid']

                    # Update order refund status
                    cursor.execute("""UPDATE orders SET status = %s, totalprice = %s, updatedat = NOW()
                                    WHERE orderid = %s""", (refund_status, refund_amount, order_id))

                    # Notify user of cancellation and refund
                    user_notification_text = f"Your service request has been cancelled. Refund status: {refund_status}."
                    if refund_amount > 0:
                        user_notification_text += f" Refund amount: ${refund_amount:.2f}"

                    cursor.execute("""INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                    VALUES (%s, %s, %s, %s, NOW())""", (userid, 'ORDER CANCELLED', user_notification_text, False))

                    cursor.execute("""INSERT INTO inappmessages (orderid, senderid, receiverid, message, timesent)
                                        VALUES (%s, %s, %s, %s, NOW())""",
                                   (order_id, 'SYSTEM', userid, user_notification_text))


                    # Notify provider
                    if tidyspid:
                        provider_notification_text = f"A service request has been cancelled. Order ID: {order_id}"

                        cursor.execute("""INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                    VALUES (%s, %s, %s, %s, NOW())""", (tidyspid, 'ORDER CANCELLED', provider_notification_text, False))

                    # Inapp message for user and service provider
                    cursor.execute("""INSERT INTO inappmessages (orderid, senderid, receiverid, message, timesent)
                        VALUES (%s, %s, %s, %s, NOW())""", (order_id, 'SYSTEM', tidyspid, provider_notification_text))

                    conn.commit()

                    # Log succes to SNS
                    sns_client.publish(
                        TopicArn=SNS_LOGGING_TOPIC_ARN,
                        Message=json.dumps({
                            "logtypeid": 1,
                            "categoryid": 9,  # Refund processed
                            "transactiontypeid": 5, # Order cancelation
                            "statusid": 7, # Refund processed
                            'userid': userid,
                            'tidyspidid': tidyspid,
                            'orderid': order_id,
                        })
                    )
                    logger.info(f"Refund successful for order {order_id}: {refund_id}")

                    results.append({
                        'orderid': order_id,
                        'status': 'SUCCESS',
                        'refund_status': refund_status,
                        'refundAmount': refund_amount if refund_amount > 0 else 0
                    })

            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"Database error processing order {order_id}: {str(e)}")

                # Log error to SNS
                sns_client.publish(
                    TopicArn=SNS_LOGGING_TOPIC_ARN,
                    Message=json.dumps({
                        "logtypeid": 4,
                        "categoryid": 28, # Payment failed
                        "transactiontypeid": 5,  # Order Cancellation
                        "statusid": 26, # Payment failed
                        'userid': userid,
                        'tidyspidid': tidyspid,
                        'orderid': order_id,
                    })
                )

                results.append({
                    'orderid': order_id,
                    'status': 'FAILED',
                    'error': str(e),
                })

            finally:
                cursor.close()
                conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps({'results':results})
        }

    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'ERROR',
                'error': str(e),
            })
        }







