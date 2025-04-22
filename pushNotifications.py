import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')
pinpoint_client = boto3.client('pinpoint', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]

# AWS Pinpoint configuration
PINPOINT_PROJECT_ID = secrets.get("PINPOINT_PROJECT_ID")


def get_user_endpoints(cursor, user_id):
    """Retrieve device endpoints for a user from the database"""
    cursor.execute("""
        SELECT endpoint_id, platform, token, enabled
        FROM user_endpoints
        WHERE userid = %s AND enabled = TRUE
    """, (user_id,))

    return cursor.fetchall()


def get_notification_template(notification_type, language="en"):
    """Get notification title and body templates based on type and language"""
    templates = {
        "ORDER_STATUS_CHANGE": {
            "en": {
                "title": "Order Status Update",
                "body": "Your order #{{order_id}} has been updated to {{status}}."
            },
            "es": {
                "title": "Actualización de estado del pedido",
                "body": "Su pedido #{{order_id}} ha sido actualizado a {{status}}."
            }
        },
        "NEW_MESSAGE": {
            "en": {
                "title": "New Message",
                "body": "You have a new message from {{sender_name}}."
            },
            "es": {
                "title": "Nuevo mensaje",
                "body": "Tienes un nuevo mensaje de {{sender_name}}."
            }
        },
        "PAYMENT_CONFIRMATION": {
            "en": {
                "title": "Payment Confirmation",
                "body": "Your payment of ${{amount}} for order #{{order_id}} has been confirmed."
            },
            "es": {
                "title": "Confirmación de pago",
                "body": "Se ha confirmado su pago de ${{amount}} para el pedido #{{order_id}}."
            }
        },
        "PROVIDER_ASSIGNED": {
            "en": {
                "title": "Provider Assigned",
                "body": "{{provider_name}} has been assigned to your service."
            },
            "es": {
                "title": "Proveedor asignado",
                "body": "{{provider_name}} ha sido asignado a su servicio."
            }
        },
        "SERVICE_REMINDER": {
            "en": {
                "title": "Service Reminder",
                "body": "Reminder: Your service is scheduled for {{scheduled_time}}."
            },
            "es": {
                "title": "Recordatorio de servicio",
                "body": "Recordatorio: Su servicio está programado para {{scheduled_time}}."
            }
        },
        "RATING_REMINDER": {
            "en": {
                "title": "Rate Your Experience",
                "body": "Please take a moment to rate your experience with your recent service."
            },
            "es": {
                "title": "Califique su experiencia",
                "body": "Tómese un momento para calificar su experiencia con su servicio reciente."
            }
        },
        "DOCUMENT_READY": {
            "en": {
                "title": "Document Ready",
                "body": "Your {{document_type}} for order #{{order_id}} is now available."
            },
            "es": {
                "title": "Documento listo",
                "body": "Su {{document_type}} para el pedido #{{order_id}} ya está disponible."
            }
        },
        "COMPLAINT_UPDATE": {
            "en": {
                "title": "Complaint Update",
                "body": "We've updated the status of your complaint #{{complaint_id}}."
            },
            "es": {
                "title": "Actualización de queja",
                "body": "Hemos actualizado el estado de su queja #{{complaint_id}}."
            }
        }
    }

    # Get template for notification type and language, fallback to English if language not available
    type_templates = templates.get(notification_type, {})
    lang_template = type_templates.get(language, type_templates.get("en", {}))

    return lang_template.get("title", "Notification"), lang_template.get("body", "You have a new notification")


def fill_template(template, data):
    """Replace placeholders in template with actual values"""
    result = template
    for key, value in data.items():
        placeholder = "{{" + key + "}}"
        result = result.replace(placeholder, str(value))
    return result


def send_aws_pinpoint_notification(endpoints, title, body, data=None):
    """Send notification through AWS Pinpoint"""
    if not endpoints or not PINPOINT_PROJECT_ID:
        return {"success": 0, "failure": 0, "endpoints": []}

    message_config = {
        'APNSMessage': {
            'Action': 'OPEN_APP',
            'Title': title,
            'Body': body,
            'Data': data
        },
        'GCMMessage': {
            'Action': 'OPEN_APP',
            'Title': title,
            'Body': body,
            'Data': data
        },
        'ADMMessage': {
            'Action': 'OPEN_APP',
            'Title': title,
            'Body': body,
            'Data': data
        }
    }

    success_count = 0
    failure_count = 0

    # Group endpoints by type to minimize API calls
    endpoint_ids_by_platform = {}
    for endpoint in endpoints:
        platform = endpoint['platform'].lower()
        endpoint_id = endpoint['endpoint_id']

        if platform not in endpoint_ids_by_platform:
            endpoint_ids_by_platform[platform] = []

        endpoint_ids_by_platform[platform].append(endpoint_id)

    # Send to each platform
    try:
        for platform, endpoint_ids in endpoint_ids_by_platform.items():
            # Create endpoint mapping
            addresses = {endpoint_id: {} for endpoint_id in endpoint_ids}

            # Send message
            response = pinpoint_client.send_messages(
                ApplicationId=PINPOINT_PROJECT_ID,
                MessageRequest={
                    'Addresses': addresses,
                    'MessageConfiguration': message_config
                }
            )

            # Process response
            delivery_result = response.get('MessageResponse', {}).get('Result', {})
            for endpoint_id, result in delivery_result.items():
                if result.get('StatusCode') == 200:
                    success_count += 1
                else:
                    failure_count += 1
                    logger.error(f"Failed to send to endpoint {endpoint_id}: {result.get('StatusMessage')}")

        logger.info(f"Pinpoint push sent. Success: {success_count}, Failure: {failure_count}")

        return {
            "success": success_count,
            "failure": failure_count,
            "endpoints": [e['endpoint_id'] for e in endpoints]
        }
    except Exception as e:
        logger.error(f"Pinpoint error: {e}")
        return {"success": 0, "failure": len(endpoints), "error": str(e),
                "endpoints": [e['endpoint_id'] for e in endpoints]}


def process_and_send_notification(user_id, notification_type, notification_data, language="en"):
    """Process and send a notification to user devices"""
    connection = None
    cursor = None

    try:
        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Get user's device endpoints
        endpoints = get_user_endpoints(cursor, user_id)

        if not endpoints:
            logger.info(f"No active endpoints found for user {user_id}")
            return {
                "status": "no_devices",
                "message": f"No active endpoints found for user {user_id}"
            }

        # Get notification template
        title_template, body_template = get_notification_template(notification_type, language)

        # Fill template with data
        title = fill_template(title_template, notification_data)
        body = fill_template(body_template, notification_data)

        # Record notification in database
        cursor.execute("""
            INSERT INTO notifications (
                userid, notificationtype, title, message, data, isread, createdat
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING notificationid
        """, (
            user_id,
            notification_type,
            title,
            body,
            json.dumps(notification_data),
            False,
            datetime.now()
        ))

        notification_id = cursor.fetchone()['notificationid']
        connection.commit()

        # Add notification ID to data for deep linking
        notification_data["notification_id"] = notification_id

        # Send via AWS Pinpoint
        result = send_aws_pinpoint_notification(endpoints, title, body, notification_data)

        # Log success to SNS
        log_to_sns(1, 7, 15, 1, {
            "user_id": user_id,
            "notification_id": notification_id,
            "notification_type": notification_type,
            "endpoints_count": len(endpoints),
            "success_count": result.get("success", 0)
        }, "Push Notification - Success", user_id)

        return {
            "status": "success",
            "notification_id": notification_id,
            "title": title,
            "body": body,
            "endpoints": len(endpoints),
            "sent": result.get("success", 0),
            "failed": result.get("failure", 0)
        }

    except Exception as e:
        logger.error(f"Failed to send notification: {e}")

        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 15, 43, {
            "user_id": user_id,
            "notification_type": notification_type,
            "error": str(e)
        }, "Push Notification - Failed", user_id)

        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def lambda_handler(event, context):
    """
    Lambda handler for sending push notifications triggered by SNS messages.

    Expected SNS message format:
    {
        "user_id": "123",
        "notification_type": "ORDER_STATUS_CHANGE",
        "notification_data": {
            "order_id": "ABC123",
            "status": "Delivered",
            "other_fields": "as needed"
        },
        "language": "en"  // Optional, defaults to English
    }
    """
    try:
        # Process SNS records
        for record in event['Records']:
            # Parse SNS message
            message = json.loads(record['Sns']['Message'])

            # Extract parameters
            user_id = message.get('user_id')
            notification_type = message.get('notification_type')
            notification_data = message.get('notification_data', {})
            language = message.get('language', 'en')

            # Validate required parameters
            if not user_id or not notification_type:
                logger.error("Missing required parameters: user_id and notification_type are required")
                continue

            # Validate notification type
            valid_notification_types = [
                "ORDER_STATUS_CHANGE", "NEW_MESSAGE", "PAYMENT_CONFIRMATION",
                "PROVIDER_ASSIGNED", "SERVICE_REMINDER", "RATING_REMINDER",
                "DOCUMENT_READY", "COMPLAINT_UPDATE"
            ]

            if notification_type not in valid_notification_types:
                logger.error(f"Invalid notification_type: {notification_type}")
                continue

            # Process and send notification
            result = process_and_send_notification(user_id, notification_type, notification_data, language)

            if result.get("status") == "error":
                logger.error(f"Failed to send notification: {result.get('message')}")
            else:
                logger.info(f"Notification sent successfully: {result}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Notifications processed successfully'
            })
        }

    except Exception as e:
        logger.error(f"Notification processing error: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error processing notifications',
                'error': str(e)
            })
        }