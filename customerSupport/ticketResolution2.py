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
SUPPORT_ANALYTICS_TOPIC_ARN = secrets.get("SUPPORT_ANALYTICS_TOPIC_ARN")


def send_resolution_confirmation(user_email, ticket_id, subject, is_resolved):
    """Send resolution confirmation email to user"""
    try:
        if not user_email:
            logger.warning(f"No email address provided for resolution confirmation")
            return False

        # Determine message based on resolution status
        if is_resolved:
            email_subject = f"Support Ticket #{ticket_id} - Resolved"
            message_header = "Your support ticket has been marked as resolved."
            message_cta = "If you continue to experience issues, you can reopen the ticket."
        else:
            email_subject = f"Support Ticket #{ticket_id} - Reopened"
            message_header = "Your support ticket has been reopened."
            message_cta = "Our support team will continue working on your issue."

        # Prepare email content
        email_body = f"""
        <html>
        <body>
            <h2>Support Ticket Update</h2>
            <p>{message_header}</p>

            <h3>Ticket Details:</h3>
            <p><strong>Ticket ID:</strong> {ticket_id}</p>
            <p><strong>Subject:</strong> {subject}</p>

            <p>{message_cta}</p>

            <p>Thank you for your feedback. It helps us improve our support services.</p>
            <p>Customer Support Team</p>
        </body>
        </html>
        """

        # Send email
        send_email_via_ses(
            user_email,
            f"Your Support Ticket #{ticket_id} Has Been {('Resolved' if is_resolved else 'Reopened')}",
            email_body
        )

        logger.info(f"Resolution confirmation email sent to user for ticket #{ticket_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to send resolution confirmation email: {str(e)}")
        return False


def notify_support_team(ticket_data, department_email):
    """Notify support team about ticket resolution or reopening"""
    try:
        ticket_id = ticket_data.get('ticket_id')
        subject = ticket_data.get('subject', 'No subject')
        is_resolved = ticket_data.get('is_resolved', True)
        feedback = ticket_data.get('feedback', 'No feedback provided')
        satisfaction_rating = ticket_data.get('satisfaction_rating')

        # Format satisfaction rating for display
        rating_display = f"{satisfaction_rating}/5" if satisfaction_rating is not None else "Not provided"

        # Determine message based on resolution status
        if is_resolved:
            email_subject = f"Ticket #{ticket_id} - Resolved by Customer"
            status_message = "The customer has marked this ticket as resolved."
        else:
            email_subject = f"Ticket #{ticket_id} - Reopened by Customer"
            status_message = "The customer has reopened this ticket. Please prioritize accordingly."

        # Prepare email content
        email_body = f"""
        <html>
        <body>
            <h2>Support Ticket Update</h2>
            <p><strong>{status_message}</strong></p>

            <h3>Ticket Details:</h3>
            <table border="1" cellpadding="5" style="border-collapse: collapse;">
                <tr><td><strong>Ticket ID:</strong></td><td>{ticket_id}</td></tr>
                <tr><td><strong>Subject:</strong></td><td>{subject}</td></tr>
                <tr><td><strong>Resolution Status:</strong></td><td>{'Resolved' if is_resolved else 'Reopened'}</td></tr>
                <tr><td><strong>Customer Feedback:</strong></td><td>{feedback}</td></tr>
                <tr><td><strong>Satisfaction Rating:</strong></td><td>{rating_display}</td></tr>
            </table>

            <p><a href="https://support.yourcompany.com/tickets/{ticket_id}">View Ticket Details</a></p>
        </body>
        </html>
        """

        # Send email
        ses_client.send_email(
            Source=secrets["SUPPORT_EMAIL_FROM"],
            Destination={'ToAddresses': [department_email]},
            Message={
                'Subject': {'Data': email_subject},
                'Body': {
                    'Html': {'Data': email_body}
                }
            }
        )

        logger.info(f"Support team notification email sent for ticket #{ticket_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to send support team notification: {str(e)}")
        return False


def lambda_handler(event, context):
    connection = None
    cursor = None
    processed_records = []

    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        for record in event['Records']:
            record_result = {
                'success': False,
                'record_id': record.get('messageId', 'unknown')
            }

            try:
                # Parse SNS message
                message = json.loads(record['Sns']['Message'])

                # Extract data
                ticket_id = message.get('ticket_id')
                user_id = message.get('user_id')
                department_id = message.get('department_id')
                is_resolved = message.get('is_resolved')
                subject = message.get('subject')
                feedback = message.get('feedback')
                satisfaction_rating = message.get('satisfaction_rating')

                if not all([ticket_id, user_id, department_id, subject]):
                    raise ValueError("Missing required ticket information")

                # Get user email for notification
                cursor.execute("SELECT email FROM users WHERE userid = %s", (user_id,))
                user = cursor.fetchone()

                if user:
                    logger.info(f"User details retrieved for user ID {user_id}")
                    user_email = user.get('email')
                else:
                    logger.warning(f"No user details found for user ID {user_id}")
                    user_email = None

                # Get department email for support team notification
                cursor.execute("SELECT department_email FROM support_departments WHERE department_id = %s",
                               (department_id,))
                department = cursor.fetchone()

                if department:
                    logger.info(f"Department details retrieved for department ID {department_id}")
                    department_email = department.get('department_email')
                else:
                    logger.warning(f"No department details found for department ID {department_id}")
                    department_email = secrets.get("SUPPORT_EMAIL_FROM")  # Fallback to default

                # Send notifications
                user_notified = False
                if user_email:
                    user_notified = send_resolution_confirmation(user_email, ticket_id, subject, is_resolved)

                team_notified = False
                if department_email:
                    team_notified = notify_support_team(message, department_email)

                # Update ticket history with notification information
                notification_data = {
                    'user_notified': user_notified,
                    'team_notified': team_notified,
                    'notification_time': datetime.now().isoformat()
                }

                cursor.execute("""
                    UPDATE ticket_history
                    SET notes = jsonb_set(notes::jsonb, '{notifications}', %s::jsonb)
                    WHERE ticket_id = %s AND action = %s
                    ORDER BY action_timestamp DESC
                    LIMIT 1
                """, (
                    json.dumps(notification_data),
                    ticket_id,
                    'Resolved' if is_resolved else 'Reopened'
                ))

                # Send analytics data if satisfaction rating provided
                if satisfaction_rating is not None and SUPPORT_ANALYTICS_TOPIC_ARN:
                    analytics_data = {
                        'ticket_id': ticket_id,
                        'user_id': user_id,
                        'department_id': department_id,
                        'satisfaction_rating': satisfaction_rating,
                        'feedback': feedback,
                        'is_resolved': is_resolved,
                        'resolution_time': datetime.now().isoformat(),
                        'event_type': 'ticket_resolution'
                    }

                    sns_client.publish(
                        TopicArn=SUPPORT_ANALYTICS_TOPIC_ARN,
                        Message=json.dumps(analytics_data),
                        Subject="Support Analytics: Ticket Resolution"
                    )

                    logger.info(f"Analytics data sent for ticket #{ticket_id}")

                connection.commit()

                # Log success
                record_result['success'] = True
                record_result['ticket_id'] = ticket_id
                record_result['notifications'] = notification_data

                logger.info(f"Successfully processed resolution notifications for ticket {ticket_id}")

            except Exception as record_error:
                logger.error(f"Error processing resolution notification: {str(record_error)}")
                record_result['error'] = str(record_error)

                # Log error
                log_to_sns(4, 21, 9, 43, record_result, "Resolution Notification Error",
                           user_id if 'user_id' in locals() else None)

            # Add result to processed records
            processed_records.append(record_result)

        # Create summary of processing
        success_count = sum(1 for r in processed_records if r.get('success', False))

        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed_count': len(processed_records),
                'success_count': success_count,
                'results': processed_records
            })
        }

    except Exception as e:
        logger.error(f"Lambda execution error: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process resolution notifications',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()