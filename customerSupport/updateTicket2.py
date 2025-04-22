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


def send_notification_to_staff(ticket_id, update_data, department_email=None):
    """Send notification to support staff about ticket update"""
    try:
        if not department_email:
            # Use default support email if department email not provided
            department_email = secrets["SUPPORT_EMAIL_FROM"]

        # Create a summary of updates
        update_summary = []

        if 'additional_info' in update_data:
            update_summary.append("• Additional information provided")

        if 'attachments' in update_data:
            num_attachments = len(update_data['attachments'])
            update_summary.append(f"• {num_attachments} new attachment(s) added")

        if 'comment' in update_data:
            update_summary.append("• New comment added")

        update_summary_text = "\n".join(update_summary)

        # Prepare email content
        email_subject = f"[Updated] Support Ticket #{ticket_id}"
        email_body = f"""
        <html>
        <body>
            <h2>Support Ticket Update</h2>
            <p>A customer has updated ticket #{ticket_id}.</p>

            <h3>Updates:</h3>
            <p>{update_summary_text}</p>

            <p>Please review the ticket at your earliest convenience.</p>
            <p><a href="https://support.yourcompany.com/tickets/{ticket_id}">View Ticket</a></p>
        </body>
        </html>
        """

        # Send email
        send_email_via_ses(
            department_email,
            email_subject,
            email_body
        )

        logger.info(f"Staff notification email sent for ticket #{ticket_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to send staff notification email: {str(e)}")
        return False


def notify_user_of_update_received(user_email, ticket_id, update_data):
    """Send confirmation email to user about their update being received"""
    try:
        if not user_email:
            logger.warning(f"No email address provided for user notification")
            return False

        # Create a summary of updates
        update_summary = []

        if 'additional_info' in update_data:
            update_summary.append("• Additional information")

        if 'attachments' in update_data:
            num_attachments = len(update_data['attachments'])
            update_summary.append(f"• {num_attachments} new attachment(s)")

        if 'comment' in update_data:
            update_summary.append("• New comment")

        update_summary_text = "\n".join(update_summary)

        # Prepare email content
        email_subject = f"Update Received for Ticket #{ticket_id}"
        email_body = f"""
        <html>
        <body>
            <h2>Support Ticket Update Confirmation</h2>
            <p>We have received your update to ticket #{ticket_id}.</p>

            <h3>Your update included:</h3>
            <p>{update_summary_text}</p>

            <p>Our support team will review your update as soon as possible.</p>
            <p><a href="https://support.yourcompany.com/my-tickets/{ticket_id}">View Your Ticket</a></p>

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

        logger.info(f"Update confirmation email sent to user for ticket #{ticket_id}")
        return True

    except Exception as e:
        logger.error(f"Failed to send update confirmation email: {str(e)}")
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
                update_data = message.get('update_data', {})

                if not ticket_id or not user_id:
                    raise ValueError("Missing required ticket information")

                # Get ticket and department details
                cursor.execute("""
                    SELECT t.ticket_id, t.subject, t.department_id, d.department_email,
                           u.email as user_email
                    FROM support_tickets t
                    JOIN support_departments d ON t.department_id = d.department_id
                    JOIN users u ON t.user_id = u.userid
                    WHERE t.ticket_id = %s
                """, (ticket_id,))

                result = cursor.fetchone()

                if result:
                    logger.info(f"Retrieved ticket details for ticket ID {ticket_id}")

                    department_email = result.get('department_email')
                    user_email = result.get('user_email')

                    # Notify support staff about the update
                    staff_notified = send_notification_to_staff(ticket_id, update_data, department_email)

                    # Send confirmation to user
                    user_notified = notify_user_of_update_received(user_email, ticket_id, update_data)

                    # Update the record with notification results
                    cursor.execute("""
                        UPDATE ticket_history
                        SET notes = jsonb_set(notes::jsonb, '{notifications}', %s::jsonb)
                        WHERE ticket_id = %s AND action = 'Updated'
                        ORDER BY action_timestamp DESC
                        LIMIT 1
                    """, (
                        json.dumps({
                            'staff_notified': staff_notified,
                            'user_notified': user_notified,
                            'notification_time': datetime.now().isoformat()
                        }),
                        ticket_id
                    ))

                    connection.commit()

                    # Log success
                    record_result['success'] = True
                    record_result['ticket_id'] = ticket_id
                    record_result['user_id'] = user_id
                    record_result['notifications'] = {
                        'staff_notified': staff_notified,
                        'user_notified': user_notified
                    }

                    logger.info(f"Successfully processed ticket update notifications for ticket {ticket_id}")
                else:
                    logger.warning(f"No ticket details found for ticket ID {ticket_id}")
                    record_result['error'] = "Ticket details not found"

            except Exception as record_error:
                logger.error(f"Error processing record: {str(record_error)}")
                record_result['error'] = str(record_error)

                # Log error
                log_to_sns(4, 21, 13, 43, record_result, "Ticket Update Notification Error",
                           user_id)

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
                'message': 'Failed to process ticket update notifications',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()