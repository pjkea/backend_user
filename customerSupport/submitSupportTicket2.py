import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

from customerSupport.layers.utils import get_secrets, get_db_connection, log_to_sns, send_email_via_ses

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')
ses_client = boto3.client('ses', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS Topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
TICKET_NOTIFICATION_TOPIC_ARN = secrets.get("TICKET_NOTIFICATION_TOPIC_ARN")


def determine_department(description, cursor):
    """
    Simple keyword-based department determination for 'other' category tickets
    In a production environment, this could use more sophisticated NLP methods
    """
    # Define keywords for each department
    departments = {
        1: ['billing', 'payment', 'charge', 'invoice', 'refund', 'subscription'],
        2: ['technical', 'error', 'bug', 'crash', 'not working', 'broken'],
        3: ['feature', 'enhancement', 'suggestion', 'improve'],
        4: ['account', 'login', 'password', 'profile', 'settings'],
        # Default department for general inquiries
        5: []
    }

    # Convert description to lowercase for case-insensitive matching
    description_lower = description.lower()

    # Check for keyword matches
    for dept_id, keywords in departments.items():
        for keyword in keywords:
            if keyword in description_lower:
                # Get department name for logging
                cursor.execute("SELECT department_name FROM support_departments WHERE department_id = %s", (dept_id,))
                dept = cursor.fetchone()

                if dept:
                    logger.info(f"Department details retrieved for department ID {dept_id}")
                    dept_name = dept['department_name']
                else:
                    logger.warning(f"No department details found for department ID {dept_id}")
                    dept_name = f"Department {dept_id}"

                logger.info(f"Assigned to {dept_name} based on keyword: '{keyword}'")
                return dept_id

    # Default to general inquiries department if no keywords match
    return 5


def send_confirmation_email(user_email, ticket_id, subject):
    """Send confirmation email to user"""
    try:
        email_body = f"""
        <html>
        <body>
            <h2>Support Ticket Confirmation</h2>
            <p>Thank you for contacting our support team. Your ticket has been received and is being processed.</p>
            <p><strong>Ticket ID:</strong> {ticket_id}</p>
            <p><strong>Subject:</strong> {subject}</p>
            <p>We'll get back to you as soon as possible. You can check the status of your ticket anytime by logging into your account.</p>
            <p>Best regards,<br>Customer Support Team</p>
        </body>
        </html>
        """

        send_email_via_ses(
            user_email,
            f'Support Ticket Confirmation: {ticket_id}',
            email_body
        )
        logger.info(f"Confirmation email sent to {user_email} for ticket {ticket_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to send confirmation email: {str(e)}")
        return False


def notify_department(department_id, ticket_data, cursor):
    """Notify appropriate department about new ticket"""
    try:
        # Get department email
        cursor.execute("SELECT department_email FROM support_departments WHERE department_id = %s", (department_id,))
        department = cursor.fetchone()

        if department:
            logger.info(f"Department details retrieved for department ID {department_id}")
            if department.get('department_email'):
                # Publish notification to SNS
                notification_data = {
                    'department_id': department_id,
                    'department_email': department['department_email'],
                    'ticket_data': ticket_data
                }

                sns_client.publish(
                    TopicArn=TICKET_NOTIFICATION_TOPIC_ARN,
                    Message=json.dumps(notification_data),
                    Subject=f"New Support Ticket: {ticket_data['ticket_id']}"
                )

                logger.info(f"Department notification sent for ticket {ticket_data['ticket_id']}")
                return True
            else:
                logger.warning(f"Department record found but no email available for department {department_id}")
                return False
        else:
            logger.warning(f"No department details found for department ID {department_id}")
            return False

    except Exception as e:
        logger.error(f"Failed to notify department: {str(e)}")
        return False


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            # Extract ticket details
            ticket_id = message.get('ticket_id')
            user_id = message.get('user_id')
            category_id = message.get('category_id')
            description = message.get('description')
            subject = message.get('subject')

            logger.info(f"Processing ticket {ticket_id} for user {user_id}")

            # Create database connection
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Determine department for 'other' category
            department_id = message.get('department_id')
            if category_id == 'other' or not department_id:
                department_id = determine_department(description, cursor)

                # Update ticket with determined department
                cursor.execute(
                    "UPDATE support_tickets SET department_id = %s, updated_at = %s WHERE ticket_id = %s",
                    (department_id, datetime.now(), ticket_id)
                )
                connection.commit()

                # Update message for logging
                message['department_id'] = department_id

            # Get user email for notification
            cursor.execute("SELECT email FROM users WHERE userid = %s", (user_id,))
            user = cursor.fetchone()

            if user:
                logger.info(f"User details retrieved for user ID {user_id}")
                if user.get('email'):
                    # Send confirmation email to user
                    send_confirmation_email(user['email'], ticket_id, subject)
                else:
                    logger.warning(f"User record found but no email available for user {user_id}")
            else:
                logger.warning(f"No user details found for user ID {user_id}")

            # Notify appropriate department
            notify_department(department_id, message, cursor)

            # Create ticket history entry
            cursor.execute("""
                INSERT INTO ticket_history 
                (ticket_id, action, action_by, action_timestamp, notes)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                ticket_id,
                'Created',
                user_id,
                datetime.now(),
                'Ticket created and assigned to department'
            ))
            connection.commit()

            # Log success
            log_data = {
                'ticket_id': ticket_id,
                'department_id': department_id,
                'category_id': category_id
            }
            log_to_sns(1, 21, 3, 27, log_data, "Support Ticket Processing", user_id)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Ticket processing completed successfully'})
        }

    except Exception as e:
        logger.error(f"Error processing support ticket: {str(e)}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error
        error_data = {
            'error': str(e),
            'ticket_id': ticket_id if 'ticket_id' in locals() else None,
            'user_id': user_id if 'user_id' in locals() else None
        }
        log_to_sns(4, 21, 3, 43, error_data, "Support Ticket Processing Error",
                   user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process support ticket',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()