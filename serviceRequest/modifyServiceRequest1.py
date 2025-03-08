import json
import boto3
import logging
from datetime import datetime

from layers.utils import get_secrets, get_db_connection, log_to_sns
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
    updates = {}

    try:
        # Parse the request body
        body = json.loads(event.get('body', '{}'))

        # Extract request parameters
        request_id = body.get('request_id')
        order_id = body.get('order_id')
        userid = body.get('userid')
        tidyspid = body.get('tidyspid')

        # Extract modification fields
        date = body.get('date')
        time = body.get('time')
        add_ons = body.get('add_ons')
        price = body.get('price')

        # Validate required fields
        if not order_id or not userid or not tidyspid:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'message': 'Missing required fields: order_id, userid, and tidyspid',
                })
            }

        # Build updates dict with only fields that were provided
        updates = {'order_id': order_id, 'userid': userid, 'tidyspid': tidyspid}
        update_fields = []
        update_values = []

        # ScheduleFor needs to be updated using date and time fields
        if date is not None or time is not None:
            # Get current values to merge with updates
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            cursor.execute(
                "SELECT ScheduleFor FROM orders WHERE OrderID = %s AND UserID = %s AND TidySPID = %s",
                (order_id, userid, tidyspid)
            )
            current_data = cursor.fetchone()

            if not current_data:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'success': False,
                        'message': f'No order found with id {order_id} for user {userid} and provider {tidyspid}',
                    })
                }

            # Extract current date and time from ScheduleFor
            current_schedule = current_data['schedulefor']

            # Update with new values if provided
            if date is not None and time is not None:
                # Both date and time provided, create new timestamp
                new_schedule = f"{date} {time}"
                update_fields.append("ScheduleFor = %s")
                update_values.append(new_schedule)
                updates['schedule_for'] = new_schedule
            elif date is not None:
                # Only date provided, keep current time
                current_time = current_schedule.strftime("%H:%M:%S")
                new_schedule = f"{date} {current_time}"
                update_fields.append("ScheduleFor = %s")
                update_values.append(new_schedule)
                updates['schedule_for'] = new_schedule
            elif time is not None:
                # Only time provided, keep current date
                current_date = current_schedule.strftime("%Y-%m-%d")
                new_schedule = f"{current_date} {time}"
                update_fields.append("ScheduleFor = %s")
                update_values.append(new_schedule)
                updates['schedule_for'] = new_schedule

        if add_ons is not None:
            # Handle add_ons in orderdetails table separately
            updates['add_ons'] = add_ons

        if price is not None:
            update_fields.append("TotalPrice = %s")
            update_values.append(price)
            updates['total_price'] = price

        # Only proceed if there are fields to update
        if not update_fields and add_ons is None:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'success': False,
                    'message': 'No fields to update were provided',
                })
            }

        # Add modification timestamp
        update_fields.append("UpdatedAt = %s")
        update_values.append(datetime.utcnow())

        # Set status to pending confirmation
        update_fields.append("Status = %s")
        update_values.append(2)
        updates['status'] = 'pending_confirmation'

        # Create database connection if not already created
        if not connection:
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Begin transaction
        connection.autocommit = False

        # Update orders table
        if update_fields:
            # Construct parameterized SQL query
            sql = f"UPDATE orders SET {', '.join(update_fields)} WHERE orderid = %s AND userID = %s AND tidyspid = %s"
            all_params = update_values + [order_id, userid, tidyspid]

            cursor.execute(sql, all_params)

            # Check if any rows were affected
            if cursor.rowcount == 0:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'success': False,
                        'message': f'No order found with id {order_id} for user {userid} and provider {tidyspid}',
                    })
                }

        # Update orderdetails table if add_ons were provided
        if add_ons is not None:
            cursor.execute("""UPDATE orderdetails SET addons = %s, updatedat = %s WHERE orderid = %s""",
                (json.dumps(add_ons) if isinstance(add_ons, dict) else add_ons, datetime.utcnow(), order_id))

        # Get original order data for comparison
        cursor.execute("""SELECT o.*, od.addons, u.email as sp_email, ud.phonenumber as sp_phone,
                  ud.firstname as sp_firstname, ud.lastname as sp_lastname, t.userid as sp_userid
            FROM orders o JOIN orderdetails od ON o.orderid = od.orderid JOIN tidysp t ON o.tidyspid = t.tidyspid
            JOIN userdetails ud ON t.userid = ud.userid JOIN users u ON t.userid = u.userid
            WHERE o.orderid = %s AND o.userid = %s AND o.tidyspid = %s
            """, (order_id, userid, tidyspid))

        order_data = cursor.fetchone()

        # Extract original values for comparison
        schedule_for = order_data.get('schedulefor')
        original_values = {
            'date': schedule_for.strftime("%Y-%m-%d") if schedule_for else None,
            'time': schedule_for.strftime("%H:%M:%S") if schedule_for else None,
            'add_ons': order_data.get('addons'),
            'price': float(order_data.get('totalprice')) if order_data.get('totalprice') else None
        }

        connection.commit()

        # Prepare message for SNS
        message = {
            'order_id': order_id,
            'request_id': request_id,
            'userid': userid,
            'tidyspid': tidyspid,
            'modifications': {k: v for k, v in updates.items() if
                              k not in ['order_id', 'userid', 'tidyspid', 'status']},
            'original_values': original_values,
            'service_provider': {
                'tidyspid': tidyspid,
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
            Subject=f'Service Request Modification: {order_id}'
        )

        data = {"order_id": order_id, "request_id": request_id, "tidyspid": tidyspid, "modifications": updates}

        log_to_sns(1, 1, 13, 49, data, 'Service Request Modification Success', userid)

        logger.info(f"Successfully initiated request modification for order_id: {order_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'message': 'Service request update initiated and pending confirmation from provider',
                'updates': updates,
                'order_id': order_id,
                'request_id': request_id
            })
        }

    except Exception as e:
        logger.error(f"Failed to initiate request modification: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error
        data = {order_id, e}
        log_to_sns(4, 1, 13, 43, data, 'Service Request Modification Failure', userid)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'message': f"Failed to initiate request modification: {str(e)}",
                'updates': updates if 'updates' in locals() else {}
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()