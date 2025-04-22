import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

from customerSupport.layers.utils import get_secrets, get_db_connection, log_to_sns

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS Topics
TICKET_UPDATE_TOPIC_ARN = secrets["TICKET_UPDATE_TOPIC_ARN"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Extract user ID from query parameters
        user_id = event.get('queryStringParameters', {}).get('userid')
        if not user_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: userid'})
            }

        # Parse request body
        body = json.loads(event.get('body', '{}'))
        ticket_id = body.get('ticket_id')

        if not ticket_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: ticket_id'})
            }

        # Extract update parameters
        additional_info = body.get('additional_info')
        new_attachments = body.get('attachments', [])
        comment = body.get('comment')

        # Validate that at least one update parameter is provided
        if not additional_info and not new_attachments and not comment:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'At least one update field is required (additional_info, attachments, or comment)'
                })
            }

        # Create database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify ticket exists and belongs to user
        cursor.execute("""
            SELECT ticket_id, status 
            FROM support_tickets 
            WHERE ticket_id = %s AND user_id = %s
        """, (ticket_id, user_id))

        ticket = cursor.fetchone()

        if ticket:
            logger.info(f"Ticket found for ticket ID {ticket_id} and user ID {user_id}")
        else:
            logger.warning(f"No ticket found for ticket ID {ticket_id} and user ID {user_id}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Ticket not found or not accessible'})
            }

        # Check if ticket is in a closed state
        if ticket['status'] in ['Closed', 'Resolved']:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': f'Cannot update ticket in {ticket["status"]} status. Please open a new ticket instead.'
                })
            }

        update_parts = []
        update_data = {}

        # Process additional info update
        if additional_info:
            # Append to existing description
            cursor.execute("SELECT description FROM support_tickets WHERE ticket_id = %s", (ticket_id,))
            result = cursor.fetchone()

            if result:
                logger.info(f"Current description retrieved for ticket ID {ticket_id}")
                current_description = result['description']
                new_description = f"{current_description}\n\n--- Additional information added on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n{additional_info}"

                # Update the ticket with new description
                cursor.execute("""
                    UPDATE support_tickets 
                    SET description = %s, updated_at = %s, status = 'Updated'
                    WHERE ticket_id = %s
                """, (new_description, datetime.now(), ticket_id))

                # Include in update data
                update_data['additional_info'] = additional_info
            else:
                logger.warning(f"Could not retrieve current description for ticket ID {ticket_id}")

        # Process new attachments
        attachment_ids = []
        if new_attachments:
            for attachment in new_attachments:
                cursor.execute("""
                    INSERT INTO ticket_attachments 
                    (ticket_id, file_url, file_name, uploaded_at)
                    VALUES (%s, %s, %s, %s)
                    RETURNING attachment_id
                """, (
                    ticket_id,
                    attachment.get('url'),
                    attachment.get('filename'),
                    datetime.now()
                ))

                result = cursor.fetchone()
                if result:
                    logger.info(f"Attachment successfully inserted for ticket ID {ticket_id}")
                    attachment_ids.append(result['attachment_id'])
                else:
                    logger.warning(f"Failed to insert attachment for ticket ID {ticket_id}")

            # Include in update data
            update_data['attachments'] = attachment_ids

        # Process comment
        comment_id = None
        if comment:
            cursor.execute("""
                INSERT INTO ticket_comments
                (ticket_id, user_id, comment_text, created_at, is_staff)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING comment_id
            """, (
                ticket_id,
                user_id,
                comment,
                datetime.now(),
                False  # is_staff flag
            ))

            result = cursor.fetchone()
            if result:
                logger.info(f"Comment successfully inserted for ticket ID {ticket_id}")
                comment_id = result['comment_id']
            else:
                logger.warning(f"Failed to insert comment for ticket ID {ticket_id}")

            # Include in update data
            update_data['comment'] = {
                'comment_id': comment_id,
                'text': comment
            }

        # Update ticket status to indicate customer update
        cursor.execute("""
            UPDATE support_tickets
            SET status = 'Pending Agent', updated_at = %s
            WHERE ticket_id = %s
        """, (datetime.now(), ticket_id))

        # Create history entry
        cursor.execute("""
            INSERT INTO ticket_history
            (ticket_id, action, action_by, action_timestamp, notes)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            ticket_id,
            'Updated',
            user_id,
            datetime.now(),
            json.dumps(update_data)
        ))

        # Commit all database changes
        connection.commit()

        # Prepare message for SNS
        update_message = {
            'ticket_id': ticket_id,
            'user_id': user_id,
            'timestamp': datetime.now().isoformat(),
            'update_type': 'customer_update',
            'update_data': update_data
        }

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=TICKET_UPDATE_TOPIC_ARN,
            Message=json.dumps(update_message),
            Subject=f'Ticket Update: {ticket_id}'
        )

        # Log successful update
        log_data = {
            'ticket_id': ticket_id,
            'update_type': list(update_data.keys())
        }
        log_to_sns(1, 21, 13, 1, log_data, "Ticket Update Success", user_id)

        logger.info(f"Successfully updated ticket {ticket_id} for user {user_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Ticket updated successfully',
                'ticket_id': ticket_id,
                'updates': update_data
            })
        }

    except Exception as e:
        logger.error(f"Error updating ticket: {str(e)}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error
        error_data = {
            'ticket_id': ticket_id if 'ticket_id' in locals() else None,
            'user_id': user_id if 'user_id' in locals() else None,
            'error': str(e)
        }
        log_to_sns(4, 21, 13, 43, error_data, "Ticket Update Error",
                   user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to update ticket',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()