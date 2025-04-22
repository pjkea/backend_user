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
TICKET_RESOLUTION_TOPIC_ARN = secrets["TICKET_RESOLUTION_TOPIC_ARN"]
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

        # Extract resolution parameters
        is_resolved = body.get('is_resolved', True)  # Default to resolved
        resolution_feedback = body.get('feedback', '')
        satisfaction_rating = body.get('satisfaction_rating')  # Rating 1-5

        # Validate satisfaction rating if provided
        if satisfaction_rating is not None:
            try:
                satisfaction_rating = int(satisfaction_rating)
                if satisfaction_rating < 1 or satisfaction_rating > 5:
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'message': 'Satisfaction rating must be between 1 and 5'})
                    }
            except (ValueError, TypeError):
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Invalid satisfaction rating format'})
                }

        # Create database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify ticket exists and belongs to user
        cursor.execute("""
            SELECT t.ticket_id, t.subject, t.status, t.department_id, d.department_name 
            FROM support_tickets t
            JOIN support_departments d ON t.department_id = d.department_id
            WHERE t.ticket_id = %s AND t.user_id = %s
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

        # Check if ticket is already closed
        if ticket['status'] == 'Closed':
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Ticket is already closed'})
            }

        # Update ticket status based on resolution confirmation
        new_status = 'Resolved' if is_resolved else 'Reopened'

        cursor.execute("""
            UPDATE support_tickets
            SET status = %s, updated_at = %s
            WHERE ticket_id = %s
        """, (new_status, datetime.now(), ticket_id))

        # Create resolution feedback record
        resolution_data = {
            'is_resolved': is_resolved,
            'feedback': resolution_feedback,
            'satisfaction_rating': satisfaction_rating
        }

        cursor.execute("""
            INSERT INTO ticket_resolutions
            (ticket_id, user_id, is_resolved, feedback, satisfaction_rating, resolution_time)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING resolution_id
        """, (
            ticket_id,
            user_id,
            is_resolved,
            resolution_feedback,
            satisfaction_rating,
            datetime.now()
        ))

        result = cursor.fetchone()
        if result:
            logger.info(f"Resolution record created for ticket ID {ticket_id}")
            resolution_id = result['resolution_id']
        else:
            logger.warning(f"Failed to create resolution record for ticket ID {ticket_id}")
            resolution_id = None

        # Create history entry
        cursor.execute("""
            INSERT INTO ticket_history
            (ticket_id, action, action_by, action_timestamp, notes)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            ticket_id,
            'Resolved' if is_resolved else 'Reopened',
            user_id,
            datetime.now(),
            json.dumps(resolution_data)
        ))

        # Commit all database changes
        connection.commit()

        # Prepare message for SNS
        resolution_message = {
            'ticket_id': ticket_id,
            'user_id': user_id,
            'subject': ticket['subject'],
            'department_id': ticket['department_id'],
            'department_name': ticket['department_name'],
            'resolution_id': resolution_id,
            'is_resolved': is_resolved,
            'feedback': resolution_feedback,
            'satisfaction_rating': satisfaction_rating,
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=TICKET_RESOLUTION_TOPIC_ARN,
            Message=json.dumps(resolution_message),
            Subject=f"Ticket Resolution: {ticket_id}"
        )

        # Log successful resolution
        log_data = {
            'ticket_id': ticket_id,
            'is_resolved': is_resolved,
            'satisfaction_rating': satisfaction_rating
        }
        log_to_sns(1, 21, 9, 1, log_data, "Ticket Resolution Confirmation", user_id)

        logger.info(f"Successfully processed resolution for ticket {ticket_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f"Ticket has been {'resolved' if is_resolved else 'reopened'}",
                'ticket_id': ticket_id,
                'status': new_status,
                'resolution_id': resolution_id
            })
        }

    except Exception as e:
        logger.error(f"Error processing ticket resolution: {str(e)}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error
        error_data = {
            'ticket_id': ticket_id if 'ticket_id' in locals() else None,
            'user_id': user_id if 'user_id' in locals() else None,
            'error': str(e)
        }
        log_to_sns(4, 21, 9, 43, error_data, "Ticket Resolution Error",
                   user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process ticket resolution',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()