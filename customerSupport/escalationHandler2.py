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


def notify_escalation_target(ticket_data, escalation_target, target_email):
    """Send notification email to the escalation target"""
    try:
        ticket_id = ticket_data.get('ticket_id')
        subject = ticket_data.get('subject', 'No subject')
        status = ticket_data.get('status')
        priority = ticket_data.get('priority')
        escalation_level = ticket_data.get('escalation_level')
        department_name = ticket_data.get('department_name', 'Unknown department')

        # Create escalation message with appropriate urgency based on level
        if escalation_level == 1:
            urgency_message = "This ticket requires your attention as it has not been processed within the expected timeframe."
        elif escalation_level == 2:
            urgency_message = "This ticket requires your IMMEDIATE attention as it has been significantly delayed."
        else:
            urgency_message = "This ticket requires URGENT executive attention. It has missed multiple service level targets."

        # Prepare email content
        email_subject = f"ESCALATED: Support Ticket #{ticket_id} - Level {escalation_level}"
        email_body = f"""
        <html>
        <body>
            <h2>Ticket Escalation Notice</h2>
            <p style="color: red; font-weight: bold;">This ticket has been escalated to you as the {escalation_target}.</p>

            <h3>Ticket Details:</h3>
            <table border="1" cellpadding="5" style="border-collapse: collapse;">
                <tr><td><strong>Ticket ID:</strong></td><td>{ticket_id}</td></tr>
                <tr><td><strong>Subject:</strong></td><td>{subject}</td></tr>
                <tr><td><strong>Department:</strong></td><td>{department_name}</td></tr>
                <tr><td><strong>Status:</strong></td><td>{status}</td></tr>
                <tr><td><strong>Priority:</strong></td><td>{priority}</td></tr>
                <tr><td><strong>Escalation Level:</strong></td><td>{escalation_level}</td></tr>
            </table>

            <p><strong>{urgency_message}</strong></p>

            <p>Please review this ticket at your earliest convenience and take appropriate action.</p>
            <p><a href="https://support.yourcompany.com/tickets/{ticket_id}?escalated=true">View Escalated Ticket</a></p>
        </body>
        </html>
        """

        # Send email
        send_email_via_ses(
            target_email,
            f"ESCALATED: Support Ticket #{ticket_id} - Level {escalation_level}",
            email_body
        )

        logger.info(f"Escalation notification sent to {escalation_target} ({target_email}) for ticket #{ticket_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to send escalation notification: {str(e)}")
        return False


def notify_customer_of_escalation(ticket_data, user_email):
    """Send notification to customer about ticket escalation"""
    try:
        ticket_id = ticket_data.get('ticket_id')
        subject = ticket_data.get('subject', 'No subject')

        # Prepare email content
        email_subject = f"Your Support Ticket #{ticket_id} Has Been Escalated"
        email_body = f"""
        <html>
        <body>
            <h2>Support Ticket Escalation</h2>
            <p>We wanted to inform you that your support ticket (#{ticket_id}) regarding "{subject}" has been escalated to a senior support member.</p>

            <p>This means your issue is now receiving higher priority attention from our team. We apologize for any delay in resolving your issue and want to assure you that we are working to address it as quickly as possible.</p>

            <p>You don't need to take any action at this time. A support representative will contact you with updates soon.</p>

            <p>Thank you for your patience,<br>Customer Support Team</p>
        </body>
        </html>
        """

        # Send email
        ses_client.send_email(
            Source=secrets["SUPPORT_EMAIL_FROM"],
            Destination={'ToAddresses': [user_email]},
            Message={
                'Subject': {'Data': email_subject},
                'Body': {
                    'Html': {'Data': email_body}
                }
            }
        )

        logger.info(f"Escalation notification sent to customer ({user_email}) for ticket #{ticket_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to send customer escalation notification: {str(e)}")
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
                escalation_level = message.get('escalation_level')
                escalation_target = message.get('escalation_target')

                if not all([ticket_id, user_id, escalation_level, escalation_target]):
                    raise ValueError("Missing required escalation information")

                # Get user email for notification
                cursor.execute("""
                    SELECT email FROM users WHERE userid = %s
                """, (user_id,))

                user = cursor.fetchone()

                if user:
                    logger.info(f"User details retrieved for user ID {user_id}")
                    user_email = user.get('email')
                else:
                    logger.warning(f"No user details found for user ID {user_id}")
                    user_email = None

                # Get target email based on escalation level and department
                if escalation_level == 1:
                    # Team lead
                    cursor.execute("""
                        SELECT team_lead_email FROM support_departments
                        WHERE department_id = %s
                    """, (department_id,))
                elif escalation_level == 2:
                    # Department manager
                    cursor.execute("""
                        SELECT manager_email FROM support_departments
                        WHERE department_id = %s
                    """, (department_id,))
                else:
                    # Support director - constant across departments
                    cursor.execute("""
                        SELECT value FROM system_settings
                        WHERE setting_key = 'support_director_email'
                    """)

                target_result = cursor.fetchone()

                if target_result:
                    logger.info(f"Target email retrieved for escalation level {escalation_level}")
                    if escalation_level <= 2:
                        target_email = target_result.get(
                            'team_lead_email' if escalation_level == 1 else 'manager_email')
                    else:
                        target_email = target_result.get('value')
                else:
                    logger.warning(f"No target email found for escalation level {escalation_level}")
                    # Fall back to default support email
                    target_email = secrets.get("SUPPORT_EMAIL_FROM")

                # Send notifications
                target_notified = False
                if target_email:
                    target_notified = notify_escalation_target(message, escalation_target, target_email)

                user_notified = False
                if user_email:
                    user_notified = notify_customer_of_escalation(message, user_email)

                # Update ticket history with notification information
                notification_data = {
                    'target_notified': target_notified,
                    'target_email': target_email,
                    'user_notified': user_notified,
                    'notification_time': datetime.now().isoformat()
                }

                cursor.execute("""
                    UPDATE ticket_history
                    SET notes = jsonb_set(notes::jsonb, '{notifications}', %s::jsonb)
                    WHERE ticket_id = %s AND action = 'Escalated'
                    ORDER BY action_timestamp DESC
                    LIMIT 1
                """, (json.dumps(notification_data), ticket_id))

                connection.commit()

                # Log success
                record_result['success'] = True
                record_result['ticket_id'] = ticket_id
                record_result['notifications'] = notification_data

                logger.info(f"Successfully processed escalation notifications for ticket {ticket_id}")

            except Exception as record_error:
                logger.error(f"Error processing escalation notification: {str(record_error)}")
                record_result['error'] = str(record_error)

                # Log error
                log_to_sns(4, 21, 7, 43, record_result, "Escalation Notification Error",
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
                'message': 'Failed to process escalation notifications',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()