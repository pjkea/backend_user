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
INVOICE_GENERATION_TOPIC_ARN = secrets["INVOICE_GENERATION_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")

        body = json.loads(event.get('body', '{}'))
        order_id = body.get('order_id')
        document_type = body.get('document_type', 'INVOICE')  # Can be 'INVOICE' or 'RECEIPT'

        # Validate required parameters
        if not user_id or not order_id:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: userid and order_id are required'
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

        # Check if a document already exists
        cursor.execute("""
            SELECT d.documentid, d.documenturl, d.createdat
            FROM documents d
            WHERE d.orderid = %s AND d.documenttype = %s
            ORDER BY d.createdat DESC
            LIMIT 1
        """, (order_id, document_type))

        existing_document = cursor.fetchone()

        # If document exists and is recent (could add more logic here)
        if existing_document:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'{document_type} already generated',
                    'document_id': existing_document['documentid'],
                    'document_url': existing_document['documenturl'],
                    'generated_at': existing_document['createdat'].isoformat(),
                    'status': 'AVAILABLE'
                })
            }

        # Get detailed order information for invoice generation
        cursor.execute("""
            SELECT 
                o.*,
                od.add_ons, 
                od.notes,
                p.paymentid, 
                p.amount as payment_amount, 
                p.status as payment_status,
                p.paymentgateway,
                p.transactionid,
                req.address, 
                req.latitude, 
                req.longitude,
                sp.productid,
                sp.productname,
                sp.producttype,
                sp.price as base_price,
                sp.duration as service_duration,
                t.userid as tidysp_userid,
                ud.firstname as tidysp_firstname,
                ud.lastname as tidysp_lastname,
                u.email as tidysp_email,
                ud.phonenumber as tidysp_phone,
                c.user_id as customer_id,
                c.firstname as customer_firstname,
                c.lastname as customer_lastname,
                c.email as customer_email,
                c.phonenumber as customer_phone
            FROM orders o
            LEFT JOIN orderdetails od ON o.orderid = od.orderid
            LEFT JOIN payments p ON o.orderid = p.orderid
            LEFT JOIN requests req ON o.requestid = req.requestid
            LEFT JOIN serviceproducts sp ON req.productid = sp.productid
            LEFT JOIN tidysp t ON o.tidyspid = t.tidyspid
            LEFT JOIN userdetails ud ON t.userid = ud.userid
            LEFT JOIN users u ON t.userid = u.userid
            LEFT JOIN (
                SELECT u.userid as user_id, u.email, ud.firstname, ud.lastname, ud.phonenumber
                FROM users u
                JOIN userdetails ud ON u.userid = ud.userid
            ) c ON o.userid = c.user_id
            WHERE o.orderid = %s
        """, (order_id,))

        order_details = cursor.fetchone()

        # Format order details for JSON serialization
        formatted_details = {}
        if order_details:
            formatted_details = dict(order_details)
            for key, value in formatted_details.items():
                if isinstance(value, datetime):
                    formatted_details[key] = value.isoformat()

            # Parse JSON strings
            if formatted_details.get('add_ons'):
                try:
                    formatted_details['add_ons'] = json.loads(formatted_details['add_ons'])
                except:
                    pass  # Keep as string if not valid JSON

        # Create a document record with pending status
        cursor.execute("""
            INSERT INTO documents (orderid, userid, documenttype, status, createdat)
            VALUES (%s, %s, %s, %s, NOW())
            RETURNING documentid
        """, (order_id, user_id, document_type, 'PENDING'))

        document_result = cursor.fetchone()
        document_id = document_result['documentid']

        connection.commit()

        # Prepare message for SNS
        message = {
            'document_id': document_id,
            'order_id': order_id,
            'user_id': user_id,
            'document_type': document_type,
            'order_details': formatted_details,
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS
        sns_client.publish(
            TopicArn=INVOICE_GENERATION_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'Generate {document_type}: {document_id}'
        )

        # Log success to SNS
        log_to_sns(1, 11, 21, 1, {
            "user_id": user_id,
            "order_id": order_id,
            "document_id": document_id,
            "document_type": document_type
        }, f"Request {document_type} - Success", user_id)

        logger.info(f"Successfully requested {document_type} generation for order {order_id}")

        return {
            'statusCode': 202,  # Accepted
            'body': json.dumps({
                'message': f'{document_type} generation request submitted successfully',
                'document_id': document_id,
                'status': 'PENDING',
                'note': f'Your {document_type.lower()} is being generated and will be available shortly.'
            })
        }

    except Exception as e:
        logger.error(f"Failed to request document generation: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 11, 21, 43, {
            "user_id": user_id,
            "order_id": order_id,
            "document_type": document_type,
            "error": str(e)
        }, f"Request {document_type} - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Failed to request {document_type} generation',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()