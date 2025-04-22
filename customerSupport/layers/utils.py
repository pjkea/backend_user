import json
import boto3
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS services
ses_client = boto3.client("ses", region_name="us-east-1")


def get_secrets():
    """Retrieve secrets from AWS Secrets Manager"""
    try:
        secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
        response = secrets_manager.get_secret_value(SecretId="customer-support-secrets")
        secrets = json.loads(response['SecretString'])
        return secrets
    except Exception as e:
        logger.error(f"Error retrieving secrets: {str(e)}")
        raise


def get_db_connection():
    """Create and return a database connection using secrets"""
    try:
        secrets = get_secrets()

        # Extract database credentials
        db_host = secrets["DB_HOST"]
        db_name = secrets["DB_NAME"]
        db_user = secrets["DB_USER"]
        db_password = secrets["DB_PASSWORD"]
        db_port = secrets["DB_PORT"]

        # Create connection
        connection = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )

        return connection
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise


def log_to_sns(log_type_id, category_id, transaction_type_id, status_id, data, subject, user_id=None):
    """Log events to SNS for monitoring and analytics"""
    try:
        # Get SNS topic ARN from secrets
        secrets = get_secrets()
        sns_logging_topic_arn = secrets["SNS_LOGGING_TOPIC_ARN"]

        # Initialize SNS client
        sns_client = boto3.client("sns", region_name="us-east-1")

        # Create log message
        log_message = {
            "logtypeid": log_type_id,
            "categoryid": category_id,
            "transactiontypeid": transaction_type_id,
            "statusid": status_id,
            "data": data,
            "timestamp": datetime.now().isoformat(),
        }

        # Add user ID if provided
        if user_id:
            log_message["userid"] = user_id

        # Publish to SNS
        sns_client.publish(
            TopicArn=sns_logging_topic_arn,
            Message=json.dumps(log_message),
            Subject=subject
        )

        logger.info(f"Successfully logged to SNS: {subject}")

    except Exception as e:
        logger.error(f"Error logging to SNS: {str(e)}")


def send_email_via_ses(email, subject, html_content, sender=None):
    """Send email using AWS SES"""
    if not email:
        logger.warning("Email address not provided")
        raise ValueError("Email address is required")

    try:
        # Default sender email if not provided
        sender_email = sender or "no-reply@tidyzon.com"

        # Create plain text version from HTML (simple conversion)
        plain_text = html_content.replace('<br>', '\n').replace('<p>', '\n').replace('</p>', '\n')
        plain_text = ''.join([i if ord(i) < 128 else ' ' for i in plain_text])

        # Send email
        response = ses_client.send_email(
            Source=sender_email,
            Destination={"ToAddresses": [email]},
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Text": {"Data": plain_text},
                    "Html": {"Data": html_content}
                }
            }
        )

        message_id = response.get('MessageId')
        logger.info(f"Email sent via SES to {email}, MessageID: {message_id}")
        return message_id
    except Exception as e:
        logger.error(f"Failed to send email via SES: {str(e)}", exc_info=True)
        raise