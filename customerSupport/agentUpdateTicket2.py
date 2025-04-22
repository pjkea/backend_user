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


def format_user_notification_email(ticket_data):
    """Format update notification for email delivery to user"""

    ticket_id = ticket_data.get('ticket_id')
    subject = ticket_data.get('subject', 'Your Support Ticket')
    changes = ticket_data.get('changes', {})
    public_comment = ticket_data.get('public_comment')

    # Format changes for display
    changes_html = ""

    if 'status' in changes:
        status_from = changes['status']['from']
        status_to = changes['status']['to']
        changes_html += f"""
        <tr>
            <td><strong>Status</strong></td>
            <td>{status_from}</td>
            <td>{status_to}</td>
        </tr>
        """

    if 'priority' in changes:
        priority_from = changes['priority']['from']
        priority_to = changes['priority']['to']
        changes_html += f"""
        <tr>
            <td><strong>Priority</strong></td>
            <td>{priority_from}</td>
            <td>{priority_to}</td>
        </tr>
        """

    if 'department' in changes:
        dept_from = changes['department']['from']['name']
        dept_to = changes['department']['to']['name']
        changes_html += f"""
        <tr>
            <td><strong>Department</strong></td>
            <td>{dept_from}</td>
            <td>{dept_to}</td>
        </tr>
        """

    # Format agent comment if provided
    comment_html = ""
    if public_comment:
        comment_html = f"""
        <h3>Support Agent Comment:</h3>
        <div style="background-color: #f5f5f5; padding: 10px; border-left: 4px solid #0078d4;">
            <p>{public_comment}</p>
        </div>
        """

    # Create the email subject
    email_subject = f"Update on Your Support Ticket #{ticket_id}"

    # Create the email body
    email_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #0078d4; padding: 20px; color: white;">
            <h1>Support Ticket Update</h1>
        </div>

        <div style="padding: 20px;">
            <p>Your support ticket has been updated by our support team.</p>

            <h3>Ticket Information:</h3>
            <p><strong>Ticket ID:</strong> {ticket_id}</p>
            <p><strong>Subject:</strong> {subject}</p>

            {f'''
            <h3>Changes Made:</h3>
            <table border="1" cellpadding="5" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f0f0f0;">
                    <th>Field</th>
                    <th>Previous Value</th>
                    <th>New Value</th>
                </tr>
                {changes_html}
            </table>
            ''' if changes_html else ''}

            {comment_html}

            <p>You can view the full details of your ticket and reply to this update by logging into our support portal.</p>

            <div style="margin: 20px 0; text-align: center;">
                <a href="https://support.yourcompany.com/tickets/{ticket_id}" 
                   style="background-color: #0078d4; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px;">
                   View Ticket Details
                </a>
            </div>

            <p>Thank you for your patience as we work to resolve your issue.</p>
            <p>The Support Team</p>
        </div>

        <div style="background-color: #f0f0f0; padding: 10px; font-size: 12px; text-align: center;">
            <p>This is an automated notification. Please do not reply to this email.</p>
        </div>
    </body>
    </html>
    """

    return email_subject, email_body


def send_push_notification(user_id, notification_data, cursor):
    """Create in-app notification for the user"""
    try:
        ticket_id = notification_data.get('ticket_id')
        subject = notification_data.get('subject')
        has_comment = 'public_comment' in notification_data and notification_data['public_comment']

        # Create notification title based on update type
        if has_comment:
            title = f"New comment on ticket #{ticket_id}"
        else:
            title = f"Your ticket #{ticket_id} has been updated"

        # Create notification message
        message = "A support agent has "
        if has_comment:
            message += "added a comment to your ticket."
        else:
            changes = notification_data.get('changes', {})
            if 'status' in changes:
                message += f"updated the status to '{changes['status']['to']}'."
            elif changes:
                message += "made updates to your ticket."
            else:
                message += "reviewed your ticket."

        # Insert notification into database
        cursor.execute("""
            INSERT INTO system_notifications
            (notification_type, reference_id, reference_type, title, message, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING notification_id
        """, (
            'ticket_update',
            ticket_id,
            'support_ticket',
            title,
            message,
            datetime.now()
        ))

        result = cursor.fetchone()

        if result:
            logger.info(f"In-app notification created for ticket ID {ticket_id}")
            notification_id = result['notification_id']

            # Link notification to user
            cursor.execute("""
                INSERT INTO user_notifications
                (user_id, notification_id, read_status, created_at)
                VALUES (%s, %s, %s, %s)
            """, (
                user_id,
                notification_id,
                False,
                datetime.now()
            ))

            logger.info(f"Notification linked to user {user_id}")
            return True, notification_id
        else:
            logger.warning(f"Failed to create in-app notification for ticket ID {ticket_id}")
            return False, None

    except Exception as e:
        logger.error(f"Error creating in-app notification: {str(e)}")
        return False, None


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
                agent_id = message.get('agent_id')

                if not all([ticket_id, user_id]):
                    raise ValueError("Missing required ticket information")

                # Get user details for notification
                cursor.execute("""
                    SELECT u.email, up.email_notifications, up.push_notifications
                    FROM users u
                    LEFT JOIN user_preferences up ON u.userid = up.user_id
                    WHERE u.userid = %s
                """, (user_id,))

                user = cursor.fetchone()

                if user:
                    logger.info(f"User details retrieved for user ID {user_id}")
                else:
                    logger.warning(f"No user details found for user ID {user_id}")

                # Initialize notification tracking
                notifications_sent = {
                    'email': False,
                    'push': False
                }

                # Send email notification if user has email and email notifications enabled
                if user and user.get('email') and user.get('email_notifications', True):
                    email_subject, email_body = format_user_notification_email(message)

                    try:
                        send_email_via_ses(
                            user['email'],
                            email_subject,
                            email_body
                        )

                        logger.info(f"Email notification sent to user {user_id} for ticket {ticket_id}")
                        notifications_sent['email'] = True
                    except Exception as email_error:
                        logger.error(f"Failed to send email notification: {str(email_error)}")

                # Send push notification if user has push notifications enabled
                if user and user.get('push_notifications', True):
                    push_success, notification_id = send_push_notification(user_id, message, cursor)
                    notifications_sent['push'] = push_success

                    if push_success:
                        logger.info(f"Push notification sent to user {user_id} for ticket {ticket_id}")
                    else:
                        logger.warning(f"Failed to send push notification to user {user_id}")

                # Update ticket history with notification information
                cursor.execute("""
                    UPDATE ticket_history
                    SET notes = jsonb_set(notes::jsonb, '{notifications}', %s::jsonb)
                    WHERE ticket_id = %s AND action = 'Agent Update'
                    ORDER BY action_timestamp DESC
                    LIMIT 1
                """, (
                    json.dumps({
                        'notifications_sent': notifications_sent,
                        'notification_time': datetime.now().isoformat()
                    }),
                    ticket_id
                ))

                # Commit all changes
                connection.commit()

                # Log success
                record_result['success'] = True
                record_result['ticket_id'] = ticket_id
                record_result['notifications_sent'] = notifications_sent

                logger.info(f"Successfully processed user notifications for ticket {ticket_id}")

            except Exception as record_error:
                logger.error(f"Error processing user notification: {str(record_error)}")
                record_result['error'] = str(record_error)

                # Roll back transaction for this record
                connection.rollback()

                # Log error
                log_to_sns(4, 21, 8, 43, record_result, "User Notification Error",
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
                'message': 'Failed to process user notifications',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()