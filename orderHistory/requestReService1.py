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
RE_SERVICE_TOPIC_ARN = secrets["RE_SERVICE_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")

        body = json.loads(event.get('body', '{}'))
        order_id = body.get('order_id')
        scheduled_datetime = body.get('scheduled_datetime')  # Optional: new date/time
        add_ons = body.get('add_ons')  # Optional: modified add-ons

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

        # Verify that the user has access to this order and it's completed
        cursor.execute("""
            SELECT o.orderid, o.status, o.userid, o.tidyspid, o.completedat, 
                   o.requestid, o.totalprice
            FROM orders o 
            WHERE o.orderid = %s AND o.userid = %s AND o.status = 'COMPLETED'
        """, (order_id, user_id))

        order = cursor.fetchone()
        if not order:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'message': 'Order not found, not completed, or not authorized to request re-service'
                })
            }

        # Get original request details for the re-service
        cursor.execute("""
            SELECT req.*, sp.productname, sp.producttype, sp.duration
            FROM requests req
            LEFT JOIN serviceproducts sp ON req.productid = sp.productid
            WHERE req.requestid = %s
        """, (order['requestid'],))

        original_request = cursor.fetchone()

        if not original_request:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'message': 'Original service request details not found'
                })
            }

        # Get order details with add-ons
        cursor.execute("""
            SELECT add_ons, notes 
            FROM orderdetails 
            WHERE orderid = %s
        """, (order_id,))

        order_details = cursor.fetchone()

        # Parse existing add-ons if not provided
        existing_add_ons = None
        if order_details and order_details.get('add_ons'):
            try:
                existing_add_ons = json.loads(order_details['add_ons'])
            except:
                existing_add_ons = order_details['add_ons']

        # Use provided add-ons or existing ones
        final_add_ons = add_ons if add_ons is not None else existing_add_ons

        # Choose scheduled datetime (new or original + 1 week)
        if not scheduled_datetime:
            # Default to 1 week from original service date
            if order['completedat']:
                original_date = order['completedat']
                # Add 7 days to the original date
                new_date = original_date.replace(day=original_date.day + 7)
                scheduled_datetime = new_date.isoformat()
            else:
                scheduled_datetime = datetime.now().isoformat()

        # Create a new request record
        cursor.execute("""
            INSERT INTO requests (
                userid, productid, price, add_ons, address, longitude, latitude, scheduledfor, createdat
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            ) RETURNING requestid
        """, (
            user_id,
            original_request['productid'],
            order['totalprice'],
            json.dumps(final_add_ons) if isinstance(final_add_ons, (list, dict)) else final_add_ons,
            original_request['address'],
            original_request['longitude'],
            original_request['latitude'],
            scheduled_datetime
        ))

        new_request = cursor.fetchone()
        new_request_id = new_request['requestid']

        # Prepare message for SNS
        message = {
            'request_id': new_request_id,
            'user_id': user_id,
            'original_order_id': order_id,
            'original_tidysp_id': order['tidyspid'],
            'service_details': {
                'product_id': original_request['productid'],
                'product_name': original_request['productname'],
                'product_type': original_request['producttype'],
                'duration': original_request['duration'],
                'address': original_request['address'],
                'price': float(order['totalprice']),
                'add_ons': final_add_ons,
                'scheduled_datetime': scheduled_datetime
            },
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS
        sns_client.publish(
            TopicArn=RE_SERVICE_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'Re-Service Request: {new_request_id}'
        )

        connection.commit()

        # Log success to SNS
        log_to_sns(1, 11, 12, 1, {
            "user_id": user_id,
            "original_order_id": order_id,
            "new_request_id": new_request_id
        }, "Request Re-Service - Success", user_id)

        logger.info(f"Successfully created re-service request {new_request_id} based on order {order_id}")

        return {
            'statusCode': 201,  # Created
            'body': json.dumps({
                'message': 'Re-service request submitted successfully',
                'request_id': new_request_id,
                'original_order_id': order_id,
                'scheduled_datetime': scheduled_datetime,
                'note': 'Your request has been submitted and is being processed.'
            })
        }

    except Exception as e:
        logger.error(f"Failed to request re-service: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 11, 12, 43, {
            "user_id": user_id,
            "order_id": order_id,
            "error": str(e)
        }, "Request Re-Service - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to request re-service',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()