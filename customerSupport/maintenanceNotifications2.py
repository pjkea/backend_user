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


def format_maintenance_notification_email(maintenance_data):
    """Format maintenance notification for email delivery"""

    maintenance_type = maintenance_data.get('maintenance_type', 'scheduled')
    title = maintenance_data.get('title', 'System Maintenance')
    description = maintenance_data.get('description', 'No details provided')
    start_time = maintenance_data.get('start_time')
    end_time = maintenance_data.get('end_time')
    affected_services = maintenance_data.get('affected_services', [])
    severity = maintenance_data.get('severity', 'medium')

    # Format affected services as a list if provided
    services_html = ""
    if affected_services:
        services_html = "<ul>"
        for service in affected_services:
            services_html += f"<li>{service}</li>"
        services_html += "</ul>"
    else:
        services_html = "<p>All services may be affected.</p>"

    # Format times for display
    try:
        start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_datetime = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        formatted_start = start_datetime.strftime("%A, %B %d, %Y at %I:%M %p %Z")
        formatted_end = end_datetime.strftime("%A, %B %d, %Y at %I:%M %p %Z")
        duration_hours = round((end_datetime - start_datetime).total_seconds() / 3600, 1)
    except (ValueError, AttributeError, TypeError):
        formatted_start = start_time
        formatted_end = end_time
        duration_hours = "Unknown"

    # Determine color and urgency based on severity
    if severity == 'high':
        severity_color = "#CC0000"
        importance = "Critical"
    elif severity == 'medium':
        severity_color = "#FF9900"
        importance = "Important"
    else:
        severity_color = "#009900"
        importance = "Routine"

    # Create the email subject
    email_subject = f"{importance} System Maintenance: {title}"

    # Create the email body
    email_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background-color: #f0f0f0; padding: 20px; border-bottom: 4px solid {severity_color};">
            <h1 style="color: #333;">System Maintenance Notification</h1>
        </div>

        <div style="padding: 20px;">
            <h2>{title}</h2>

            <p style="background-color: #f9f9f9; padding: 10px; border-left: 4px solid {severity_color};">
                <strong>Type:</strong> {maintenance_type.capitalize()}<br>
                <strong>Severity:</strong> {severity.capitalize()}<br>
                <strong>Start Time:</strong> {formatted_start}<br>
                <strong>End Time:</strong> {formatted_end}<br>
                <strong>Estimated Duration:</strong> {duration_hours} hours
            </p>

            <h3>Description</h3>
            <p>{description}</p>

            <h3>Affected Services</h3>
            {services_html}

            <h3>What to Expect</h3>
            <p>During this maintenance period, you may experience temporary service interruptions 
            or degraded performance. We recommend planning your activities accordingly.</p>

            <p>We apologize for any inconvenience this may cause and appreciate your understanding 
            as we work to improve our systems.</p>

            <p>If you have any questions or concerns, please contact our support team.</p>
        </div>

        <div style="background-color: #f0f0f0; padding: 10px; font-size: 12px; text-align: center;">
            <p>This is an automated notification. Please do not reply to this email.</p>
        </div>
    </body>
    </html>
    """

    return email_subject, email_body


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

                # Extract maintenance data
                maintenance_id = message.get('maintenance_id')

                if not maintenance_id:
                    raise ValueError("Missing required maintenance information")

                # Prepare for notification tracking
                notification_counts = {
                    'email': 0,
                    'sms': 0,
                    'in_app': 0,
                    'failed': 0
                }

                # Create email notification content
                email_subject, email_body = format_maintenance_notification_email(message)

                # Create in-app notification
                cursor.execute("""
                    INSERT INTO system_notifications
                    (notification_type, reference_id, reference_type, title, message, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING notification_id
                """, (
                    'maintenance',
                    maintenance_id,
                    'system_maintenance',
                    message.get('title'),
                    message.get('description'),
                    datetime.now()
                ))

                notification_result = cursor.fetchone()

                if notification_result:
                    logger.info(f"In-app notification created for maintenance ID {maintenance_id}")
                    notification_counts['in_app'] += 1
                else:
                    logger.warning(f"Failed to create in-app notification for maintenance ID {maintenance_id}")

                # Fetch users to notify based on affected services
                affected_services = message.get('affected_services', [])

                # If specific services are affected, notify users of those services
                if affected_services:
                    affected_services_str = ', '.join([f"'{service}'" for service in affected_services])
                    query = f"""
                        SELECT DISTINCT u.userid, u.email, u.phone_number, 
                               up.email_notifications, up.sms_notifications
                        FROM users u
                        JOIN user_preferences up ON u.userid = up.user_id
                        JOIN user_services us ON u.userid = us.user_id
                        WHERE us.service_name IN ({affected_services_str})
                        AND u.active = TRUE
                    """
                    cursor.execute(query)
                else:
                    # If no specific services, notify all active users
                    cursor.execute("""
                        SELECT DISTINCT u.userid, u.email, u.phone_number, 
                               up.email_notifications, up.sms_notifications
                        FROM users u
                        JOIN user_preferences up ON u.userid = up.user_id
                        WHERE u.active = TRUE
                    """)

                users = cursor.fetchall()

                if users:
                    logger.info(f"Found {len(users)} users to notify about maintenance ID {maintenance_id}")

                    # Create user notification records and send emails
                    for user in users:
                        # Record user notification
                        cursor.execute("""
                            INSERT INTO user_notifications
                            (user_id, notification_id, read_status, created_at)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            user['userid'],
                            notification_result['notification_id'] if notification_result else None,
                            False,
                            datetime.now()
                        ))

                        # Send email notification if user has email notifications enabled
                        if user.get('email_notifications', True) and user.get('email'):
                            try:
                                send_email_via_ses(
                                    user['email'],
                                    email_subject,
                                    email_body
                                )
                                notification_counts['email'] += 1
                            except Exception as email_error:
                                logger.error(
                                    f"Failed to send email notification to {user['email']}: {str(email_error)}")
                                notification_counts['failed'] += 1

                        # SMS notifications would be handled here if implemented
                        # if user.get('sms_notifications', False) and user.get('phone_number'):
                        #     # Send SMS via your preferred service
                        #     notification_counts['sms'] += 1
                else:
                    logger.warning(f"No users found to notify about maintenance ID {maintenance_id}")

                # Update maintenance record with notification stats
                cursor.execute("""
                    UPDATE system_maintenance
                    SET notification_stats = %s, notifications_sent_at = %s
                    WHERE maintenance_id = %s
                """, (
                    json.dumps(notification_counts),
                    datetime.now(),
                    maintenance_id
                ))

                # Commit all changes
                connection.commit()

                # Log success
                record_result['success'] = True
                record_result['maintenance_id'] = maintenance_id
                record_result['notification_counts'] = notification_counts

                logger.info(f"Successfully sent notifications for maintenance ID {maintenance_id}")

            except Exception as record_error:
                logger.error(f"Error processing maintenance notification: {str(record_error)}")
                record_result['error'] = str(record_error)

                # Roll back transaction for this record
                connection.rollback()

                # Log error
                log_to_sns(4, 3, 12, 43, record_result, "Maintenance Notification Processing Error", None)

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
                'message': 'Failed to process maintenance notifications',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()