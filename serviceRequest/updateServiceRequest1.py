import json
import boto3
import logging
import psycopg2
from datetime import datetime

from layers.utils import get_secrets, get_db_connection
from psycopg2.extras import RealDictCursor

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    connection = None
    cursor = None
    updates = {}

    try:
        # Retrieve changed details
        body = json.loads(event.get('body', '{}'))
        request_id = body.get('request_id')  # Added request_id field
        userid = body.get('userid')
        date = body.get('date')
        time = body.get('time')
        add_ons = body.get('add_ons')
        price = body.get('price')

        # Validate required fields
        if not request_id or not userid:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'message': 'Missing required fields: request_id and userid',
                })
            }

        # Build updates dict with only fields that were provided
        updates = {'request_id': request_id, 'userid': userid}
        update_fields = []
        update_values = []

        if date is not None:
            updates['date'] = date
            update_fields.append("date = %s")
            update_values.append(date)

        if time is not None:
            updates['time'] = time
            update_fields.append("time = %s")
            update_values.append(time)

        if add_ons is not None:
            updates['add_ons'] = add_ons
            update_fields.append("add_ons = %s")
            update_values.append(json.dumps(add_ons) if isinstance(add_ons, dict) else add_ons)

        if price is not None:
            updates['price'] = price
            update_fields.append("price = %s")
            update_values.append(price)

        # Only proceed if there are fields to update
        if not update_fields:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'message': 'No fields to update were provided',
                })
            }

        # Add modification timestamp
        update_fields.append("modified_at = %s")
        update_values.append(datetime.utcnow())
        update_fields.append("status = %s")  # Set status to 'pending confirmation'
        update_values.append('pending_confirmation')
        updates['status'] = 'pending_confirmation'

        # Update requests table
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Construct dynamic SQL query
        sql = f"UPDATE requests SET {', '.join(update_fields)} WHERE request_id = %s AND userid = %s"
        update_values.extend([request_id, userid])

        cursor.execute(sql, update_values)

        # Check if any rows were affected
        if cursor.rowcount == 0:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'success': False,
                    'message': f'No request found with id {request_id} for user {userid}',
                })
            }

        connection.commit()

        logger.info(f"Successfully changed request details for request_id: {request_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'message': 'Service request updated successfully',
                'updates': updates,
                'update_fields': update_fields,
                'update_values': update_values,
            }),
        }

    except Exception as e:
        logger.error(f"Failed to change request details: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'message': f"Failed to change request details: {str(e)}",
                'updates': updates,
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()