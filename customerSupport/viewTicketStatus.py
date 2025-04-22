import json
import boto3
import logging
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

        # Parse request body for optional parameters
        body = json.loads(event.get('body', '{}'))
        ticket_id = body.get('ticket_id')

        # Initialize response object
        response_data = {}

        # Create database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        if ticket_id:
            # Get details for a specific ticket
            cursor.execute("""
                SELECT t.ticket_id, t.subject, t.description, t.created_at, t.updated_at, 
                       t.status, t.priority, c.category_name, d.department_name,
                       (SELECT COUNT(*) FROM ticket_attachments WHERE ticket_id = t.ticket_id) as attachment_count
                FROM support_tickets t
                LEFT JOIN support_ticket_categories c ON t.category_id = c.category_id
                LEFT JOIN support_departments d ON t.department_id = d.department_id
                WHERE t.ticket_id = %s AND t.user_id = %s
            """, (ticket_id, user_id))

            ticket = cursor.fetchone()

            if ticket:
                logger.info(f"Ticket details retrieved for ticket ID {ticket_id}")

                # Get ticket history
                cursor.execute("""
                    SELECT action, action_by, action_timestamp, notes
                    FROM ticket_history
                    WHERE ticket_id = %s
                    ORDER BY action_timestamp DESC
                """, (ticket_id,))

                history = cursor.fetchall()

                if history:
                    logger.info(f"Ticket history retrieved for ticket ID {ticket_id}")
                else:
                    logger.warning(f"No ticket history found for ticket ID {ticket_id}")

                # Get attachments
                cursor.execute("""
                    SELECT attachment_id, file_name, file_url, uploaded_at
                    FROM ticket_attachments
                    WHERE ticket_id = %s
                """, (ticket_id,))

                attachments = cursor.fetchall()

                if attachments:
                    logger.info(f"Attachments retrieved for ticket ID {ticket_id}")
                else:
                    logger.info(f"No attachments found for ticket ID {ticket_id}")

                # Get comments/replies
                cursor.execute("""
                    SELECT comment_id, user_id, comment_text, created_at, is_staff
                    FROM ticket_comments
                    WHERE ticket_id = %s
                    ORDER BY created_at ASC
                """, (ticket_id,))

                comments = cursor.fetchall()

                if comments:
                    logger.info(f"Comments retrieved for ticket ID {ticket_id}")
                else:
                    logger.info(f"No comments found for ticket ID {ticket_id}")

                # Construct response with all ticket details
                response_data = {
                    'ticket': ticket,
                    'history': history,
                    'attachments': attachments,
                    'comments': comments
                }
            else:
                logger.warning(f"No ticket found with ID {ticket_id} for user {user_id}")
                return {
                    'statusCode': 404,
                    'body': json.dumps({'message': 'Ticket not found'})
                }
        else:
            # Get all tickets for the user
            cursor.execute("""
                SELECT t.ticket_id, t.subject, t.created_at, t.updated_at, t.status, 
                       t.priority, c.category_name
                FROM support_tickets t
                LEFT JOIN support_ticket_categories c ON t.category_id = c.category_id
                WHERE t.user_id = %s
                ORDER BY 
                    CASE 
                        WHEN t.status = 'New' THEN 1
                        WHEN t.status = 'In Progress' THEN 2
                        WHEN t.status = 'Pending Customer' THEN 3
                        WHEN t.status = 'Resolved' THEN 4
                        WHEN t.status = 'Closed' THEN 5
                        ELSE 6
                    END,
                    t.updated_at DESC
            """, (user_id,))

            tickets = cursor.fetchall()

            if tickets:
                logger.info(f"Retrieved {len(tickets)} tickets for user {user_id}")
            else:
                logger.info(f"No tickets found for user {user_id}")

            response_data = {
                'tickets': tickets
            }

        # Log successful access
        log_data = {
            'user_id': user_id,
            'ticket_id': ticket_id,
            'action': 'view_ticket_status'
        }
        log_to_sns(1, 21, 5, 1, log_data, "Ticket Status Viewed", user_id)

        logger.info(f"Successfully retrieved ticket information for user {user_id}")

        return {
            'statusCode': 200,
            'body': json.dumps(response_data)
        }

    except Exception as e:
        logger.error(f"Error retrieving ticket information: {str(e)}")

        # Log error
        error_data = {
            'user_id': user_id if 'user_id' in locals() else None,
            'ticket_id': ticket_id if 'ticket_id' in locals() else None,
            'error': str(e)
        }
        log_to_sns(4, 21, 5, 43, error_data, "Ticket Status View Error",
                   user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to retrieve ticket information',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()