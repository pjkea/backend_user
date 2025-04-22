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
MAINTENANCE_NOTIFICATION_TOPIC_ARN = secrets["MAINTENANCE_NOTIFICATION_TOPIC_ARN"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Extract admin user ID from query parameters
        admin_id = event.get('queryStringParameters', {}).get('adminid')
        if not admin_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: adminid'})
            }

        # Parse request body
        body = json.loads(event.get('body', '{}'))

        # Extract maintenance details
        maintenance_type = body.get('maintenance_type')  # e.g., 'scheduled', 'emergency'
        title = body.get('title')
        description = body.get('description')
        start_time = body.get('start_time')  # ISO format datetime string
        end_time = body.get('end_time')  # ISO format datetime string
        affected_services = body.get('affected_services', [])
        severity = body.get('severity', 'medium')  # low, medium, high

        # Validate required fields
        if not all([maintenance_type, title, description, start_time, end_time]):
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required fields: maintenance_type, title, description, start_time, and end_time are required'
                })
            }

        # Parse datetime strings
        try:
            start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_datetime = datetime.fromisoformat(end_time.replace('Z', '+00:00'))

            # Validate time range
            if end_datetime <= start_datetime:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'End time must be after start time'})
                }
        except ValueError:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid datetime format. Use ISO format (e.g., 2025-02-15T14:30:00Z)'})
            }

        # Create database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify admin permissions
        cursor.execute("SELECT role FROM users WHERE userid = %s", (admin_id,))
        user_role = cursor.fetchone()

        if user_role:
            logger.info(f"User role retrieved for admin ID {admin_id}")

            if user_role['role'] not in ['admin', 'system_admin', 'support_manager']:
                return {
                    'statusCode': 403,
                    'body': json.dumps({'message': 'Insufficient permissions to schedule maintenance notifications'})
                }
        else:
            logger.warning(f"No user found for admin ID {admin_id}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Admin user not found'})
            }

        # Create maintenance record
        cursor.execute("""
            INSERT INTO system_maintenance
            (maintenance_type, title, description, start_time, end_time, 
             affected_services, severity, created_by, created_at, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING maintenance_id
        """, (
            maintenance_type,
            title,
            description,
            start_datetime,
            end_datetime,
            json.dumps(affected_services),
            severity,
            admin_id,
            datetime.now(),
            'Scheduled'
        ))

        result = cursor.fetchone()

        if result:
            logger.info(f"Maintenance record created with ID {result['maintenance_id']}")
            maintenance_id = result['maintenance_id']
        else:
            logger.warning("Failed to create maintenance record")
            return {
                'statusCode': 500,
                'body': json.dumps({'message': 'Failed to create maintenance record'})
            }

        # Prepare notification message
        notification_data = {
            'maintenance_id': maintenance_id,
            'maintenance_type': maintenance_type,
            'title': title,
            'description': description,
            'start_time': start_time,
            'end_time': end_time,
            'affected_services': affected_services,
            'severity': severity,
            'created_by': admin_id,
            'created_at': datetime.now().isoformat()
        }

        # Publish to SNS for notification processing
        sns_client.publish(
            TopicArn=MAINTENANCE_NOTIFICATION_TOPIC_ARN,
            Message=json.dumps(notification_data),
            Subject=f"System Maintenance: {title}"
        )

        # Commit database changes
        connection.commit()

        # Log successful creation
        log_data = {
            'maintenance_id': maintenance_id,
            'maintenance_type': maintenance_type,
            'start_time': start_time,
            'end_time': end_time,
            'severity': severity
        }
        log_to_sns(1, 3, 12, 1, log_data, "Maintenance Notification Created", admin_id)

        logger.info(f"Successfully created maintenance notification with ID {maintenance_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Maintenance notification scheduled successfully',
                'maintenance_id': maintenance_id
            })
        }

    except Exception as e:
        logger.error(f"Error creating maintenance notification: {str(e)}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error
        error_data = {
            'admin_id': admin_id if 'admin_id' in locals() else None,
            'error': str(e)
        }
        log_to_sns(4, 3, 12, 43, error_data, "Maintenance Notification Error",
                   admin_id if 'admin_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to create maintenance notification',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()