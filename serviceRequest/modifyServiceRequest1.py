import json
import boto3
import psycopg2
import logging
from datetime import datetime

from serviceRequest.layers.utils import get_secrets, get_db_connection
from psycopg2.extras import RealDictCursor

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']
REQUEST_MODIFICATION_TOPIC_ARN = secrets["REQUEST_MODIFICATION_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        body = json.loads(event.get('body', '{}'))
        request_id = body.get('request_id')
        order_id = body.get('order_id')
        userid = body.get('userid')
        tidyspid = body.get('tidyspid')
        updates = body.get('updates')
        update_fields = body.get('update_fields')
        update_values = body.get('update_values')

        if not order_id or not userid or not request_id:
            return {'message': f'Missing required fields {order_id} and {userid} and {request_id}'}


        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Retrieve original details
        cursor.execute("""
                    SELECT o.*, t.tidyspid, t.userid as sp_userid, 
                           ud.firstname as sp_firstname, ud.lastname as sp_lastname,
                           u.email as sp_email, ud.phonenumber as sp_phone
                    FROM order o
                    JOIN tidysp t ON o.tidyspid = t.tidyspid
                    JOIN userdetails ud ON t.userid = ud.userid
                    JOIN users u ON t.userid = u.userid
                    WHERE o.order_id = %s AND r.userid = %s AND t.tidyspid = %s
                """, (request_id, userid, tidyspid))

        order_data = cursor.fetchone()

        if not order_data:
            return {'statusCode': 404,'message': f'Order {order_id} not found'}

        # Store original values for comparison
        original_values = {
            'date': order_data.get('date'),
            'time': order_data.get('time'),
            'add_ons': order_data.get('add_ons'),
            'price': order_data.get('price')
        }

        # Construct dynamic SQL query
        sql = f"UPDATE order SET{','.join(update_fields)} WHERE order_id = %s AND userid = %s AND tidyspid = %s"
        update_values.extend([request_id, userid])

        cursor.execute(sql, (order_id, userid, tidyspid))
        connection.commit()

        # Get service provider details for notification
        tidysp_id = order_data.get('tidyspid')

        if not tidysp_id:
            logger.warning(f"No service provider associated with request {order_id}")

        # Prepare message for SNS
        message = {
            'order_id': order_id,
            'userid': userid,
            'tidyspid': tidysp_id,
            'modifications': {k: v for k, v in updates.items() if k not in ['request_id', 'userid', 'status']},
            'original_values': original_values,
            'service_provider': {
                'tidyspid': tidysp_id,
                'userid': order_data.get('sp_userid'),
                'firstname': order_data.get('sp_firstname'),
                'lastname': order_data.get('sp_lastname'),
                'email': order_data.get('sp_email'),
                'phone': order_data.get('sp_phone')
            },
            'timestamp': datetime.utcnow().isoformat()
        }

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=REQUEST_MODIFICATION_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'Service Request Modification: {request_id}'
        )

        # Log success
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 1,
                "categoryid": 2,  # Service Request Modification
                "transactiontypeid": 13,
                "statusid": 34,  # Request started
                "userid": userid,
                "request_id": request_id,
                "tidyspid": tidysp_id,
                "modifications": updates
            }),
            Subject=f'Service Request Modification Success: {request_id}'
        )

        logger.info(f"Successfully initiated request modification for request_id: {request_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'message': 'Service request update initiated and pending confirmation from provider',
                'updates': updates,
                'request_id': request_id
            })
        }

    except Exception as e:
        logger.error(f"Failed to initiate request modification for order id: {order_id}"
                     f"error: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log eror
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 4,
                "categoryid": 2,  # Service Request Modification
                "transactiontypeid": 13,
                "statusid": 43,  # Failed
                "userid": userid if 'userid' in locals() else "unknown",
                "request_id": request_id if 'request_id' in locals() else "unknown",
                "error": str(e)
            }),
            Subject='Service Request Modification Failure'
        )

        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'message': f"Failed to initiate request modification: {str(e)}",
                'updates': updates
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()





