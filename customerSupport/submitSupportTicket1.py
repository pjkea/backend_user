import json
import boto3
import logging
import uuid
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
TICKET_PROCESSING_TOPIC_ARN = secrets["TICKET_PROCESSING_TOPIC_ARN"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))

        # Extract user ID from query parameters
        user_id = event.get('queryStringParameters', {}).get('userid')
        if not user_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: userid'})
            }

        # Extract ticket details
        subject = body.get('subject')
        description = body.get('description')
        category_id = body.get('category_id')
        priority = body.get('priority', 'Medium')
        attachments = body.get('attachments', [])

        # Validate required fields
        if not subject or not description or not category_id:
            return {
                'statusCode': 400,
                'body': json.dumps(
                    {'message': 'Missing required fields: subject, description, and category_id are required'})
            }

        # Create database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify category exists
        cursor.execute(
            "SELECT category_id, category_name, department_id FROM support_ticket_categories WHERE category_id = %s",
            (category_id,))
        category = cursor.fetchone()

        if category:
            logger.info(f"Category details retrieved for category ID {category_id}")
        else:
            logger.warning(f"No category details found for category ID {category_id}")

        if not category and category_id != 'other':
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid category ID'})
            }

        # Generate a unique ticket ID
        ticket_id = str(uuid.uuid4())

        # Current timestamp
        current_time = datetime.now()

        # Determine department ID
        department_id = None
        if category_id == 'other':
            # For 'other' category, department will be assigned in the second Lambda
            department_id = None
        else:
            department_id = category.get('department_id')

        # Insert ticket into database
        cursor.execute("""
            INSERT INTO support_tickets 
            (ticket_id, user_id, subject, description, category_id, priority, status, 
            created_at, updated_at, department_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING ticket_id
        """, (
            ticket_id,
            user_id,
            subject,
            description,
            category_id,
            priority,
            'New',  # Initial status
            current_time,
            current_time,
            department_id
        ))

        # Get the inserted ticket ID
        inserted_ticket = cursor.fetchone()

        if inserted_ticket:
            logger.info(f"Ticket successfully inserted with ID {ticket_id}")
        else:
            logger.warning(f"No confirmation of ticket insertion for ID {ticket_id}")

        # Save attachments if provided
        if attachments:
            for attachment in attachments:
                cursor.execute("""
                    INSERT INTO ticket_attachments 
                    (ticket_id, file_url, file_name, uploaded_at)
                    VALUES (%s, %s, %s, %s)
                """, (
                    ticket_id,
                    attachment.get('url'),
                    attachment.get('filename'),
                    current_time
                ))

        # Commit the transaction
        connection.commit()

        # Prepare message for SNS
        ticket_data = {
            'ticket_id': ticket_id,
            'user_id': user_id,
            'subject': subject,
            'description': description,
            'category_id': category_id,
            'priority': priority,
            'status': 'New',
            'created_at': current_time.isoformat(),
            'department_id': department_id,
            'attachments': attachments
        }

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=TICKET_PROCESSING_TOPIC_ARN,
            Message=json.dumps(ticket_data),
            Subject='New Support Ticket'
        )

        # Log success
        log_data = {
            'ticket_id': ticket_id,
            'category_id': category_id,
            'priority': priority
        }
        log_to_sns(1, 21, 3, 1, log_data, "Support Ticket Creation", user_id)

        logger.info(f"Successfully created support ticket: {ticket_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Support ticket submitted successfully',
                'ticket_id': ticket_id,
                'estimated_response_time': '24 hours'  # This could be dynamic based on ticket priority
            })
        }

    except Exception as e:
        logger.error(f"Error creating support ticket: {str(e)}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error
        error_data = {
            'user_id': user_id if 'user_id' in locals() else None,
            'error': str(e)
        }
        log_to_sns(4, 21, 3, 43, error_data, "Support Ticket Creation Error",
                   user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to create support ticket',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()