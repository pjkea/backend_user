import json
import boto3
import logging
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
COMPLAINT_TOPIC_ARN = secrets["COMPLAINT_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")

        body = json.loads(event.get('body', '{}'))
        order_id = body.get('order_id')
        complaint_type = body.get('complaint_type')
        complaint_text = body.get('complaint_text')
        severity = body.get('severity', 'MEDIUM')  # Default severity level

        # Validate required parameters
        if not user_id or not order_id or not complaint_type or not complaint_text:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: userid, order_id, complaint_type, and complaint_text are required'
                })
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # First verify that the user has access to this order
        cursor.execute("""
            SELECT o.orderid, o.status, o.userid, o.tidyspid, o.completedat, o.totalprice
            FROM orders o 
            WHERE o.orderid = %s AND (o.userid = %s OR o.tidyspid = %s)
        """, (order_id, user_id, user_id))

        order = cursor.fetchone()
        if not order:
            return {
                'statusCode': 403,
                'body': json.dumps({
                    'message': 'Unauthorized access to this order'
                })
            }

        # Determine if user is a customer or service provider
        is_customer = order['userid'] == user_id

        # Create a complaint record
        cursor.execute("""
            INSERT INTO complaints (orderid, userid, complainttype, description, severity, status, createdat) 
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            RETURNING complaintid
        """, (order_id, user_id, complaint_type, complaint_text, severity, 'PENDING'))

        complaint_result = cursor.fetchone()
        complaint_id = complaint_result['complaintid']

        # Create a support ticket
        cursor.execute("""
            INSERT INTO supporttickets (complaintid, status, priority, createdat) 
            VALUES (%s, %s, %s, NOW())
            RETURNING ticketid
        """, (complaint_id, 'OPEN', severity))

        ticket_result = cursor.fetchone()
        ticket_id = ticket_result['ticketid']

        connection.commit()

        # Prepare message for SNS
        message = {
            'complaint_id': complaint_id,
            'ticket_id': ticket_id,
            'order_id': order_id,
            'user_id': user_id,
            'is_customer': is_customer,
            'complaint_type': complaint_type,
            'complaint_text': complaint_text,
            'severity': severity,
            'order_details': {
                'status': order['status'],
                'completed_at': order['completedat'].isoformat() if order['completedat'] else None,
                'total_price': float(order['totalprice']) if order['totalprice'] else 0
            },
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS
        sns_client.publish(
            TopicArn=COMPLAINT_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'New Complaint: {complaint_id}'
        )

        # Log success to SNS
        log_to_sns(1, 11, 20, 1, {
            "user_id": user_id,
            "order_id": order_id,
            "complaint_id": complaint_id,
            "ticket_id": ticket_id
        }, "Submit Complaint - Success", user_id)

        logger.info(f"Successfully submitted complaint for order {order_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Complaint submitted successfully',
                'complaint_id': complaint_id,
                'ticket_id': ticket_id,
                'status': 'PENDING',
                'note': 'Your complaint has been received and will be processed shortly. You will be notified once it is reviewed.'
            })
        }

    except Exception as e:
        logger.error(f"Failed to submit complaint: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 11, 20, 43, {
            "user_id": user_id,
            "order_id": order_id,
            "error": str(e)
        }, "Submit Complaint - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to submit complaint',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()