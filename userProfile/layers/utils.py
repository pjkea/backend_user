import json
import boto3
import psycopg2
import logging
import hashlib
import os
import re
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from twilio.rest import Client

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")
ses_client = boto3.client("ses", region_name="us-east-1")
s3_client = boto3.client('s3', region_name='us-east-1')


# Function to load secrets from AWS Secrets Manager
def get_secrets():
    try:
        secrets = json.loads(secrets_client.get_secret_value(SecretId="tidyzon-env-variables")["SecretString"])
        return secrets
    except ClientError as e:
        logger.error(f"AWS Secrets Manager error: {e.response['Error']['Message']}", exc_info=True)
        raise


# Load secrets for global use
secrets = get_secrets()

# Twilio credentials - load them once at module level for efficiency
TWILIO_ACCOUNT_SID = secrets.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = secrets.get("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = secrets.get("TWILIO_PHONE_NUMBER")

# Initialize Twilio Client if credentials are available
twilio_client = None
if all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
    twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


# SNS Topic ARNs
SNS_LOGGING_TOPIC_ARN = secrets.get("SNS_LOGGING_TOPIC_ARN")

# Stripe API Key
STRIPE_API_KEY = secrets.get("STRIPE_API_KEY")

# Password requirements
PASSWORD_MIN_LENGTH = 8
PASSWORD_REGEX = r'^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$'
PASSWORD_REQUIREMENTS = [
    "At least 8 characters long",
    "Contains at least one uppercase letter",
    "Contains at least one lowercase letter",
    "Contains at least one number",
    "Contains at least one special character (@$!%*?&)"
]


# Function to establish a database connection
def get_db_connection():
    try:
        connection = psycopg2.connect(
            host=secrets["DB_HOST"],
            database=secrets["DB_NAME"],
            user=secrets["DB_USER"],
            password=secrets["DB_PASSWORD"],
            port=secrets["DB_PORT"]
        )
        logger.info("Database connection established successfully")
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}", exc_info=True)
        raise


# Function to log events to AWS SNS
def log_to_sns(log_type_id, category_id, transaction_type_id, status_id, data, subject_prefix, user_id=None):
    """
    Log events to SNS for centralized logging and monitoring

    Parameters:
    - log_type_id: Type of log (1: Info, 2: Debug, 3: Warning, 4: Error)
    - category_id: Business category (1: Authentication, 2: Orders, etc.)
    - transaction_type_id: Type of transaction (1: Create, 2: Read, etc.)
    - status_id: Status of the transaction (1: Success, 2: Failed, etc.)
    - data: Additional data to log
    - subject_prefix: Prefix for the SNS subject
    - user_id: Optional user ID associated with the log
    """
    try:
        if not SNS_LOGGING_TOPIC_ARN:
            logger.warning("SNS_LOGGING_TOPIC_ARN not found in secrets")
            return

        # Format the message
        message = {
            "logtypeid": log_type_id,
            "categoryid": category_id,
            "transactiontypeid": transaction_type_id,
            "statusid": status_id,
            "data": data,
            "userid": user_id,
            "timestamp": datetime.now().isoformat()
        }

        # Create a descriptive subject based on the log type
        log_types = {1: "INFO", 2: "DEBUG", 3: "WARNING", 4: "ERROR"}
        log_type_str = log_types.get(log_type_id, "INFO")

        subject = f"{subject_prefix} - {log_type_str}" if subject_prefix else log_type_str

        # Publish to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=subject
        )

        logger.info(f"Event logged to SNS: {subject}")
    except Exception as e:
        logger.error(f"Failed to log to SNS: {str(e)}", exc_info=True)


# Function to send SMS using Twilio
def send_sms_via_twilio(phone_number, message):
    """
    Send SMS using Twilio

    Parameters:
    - phone_number: Recipient's phone number
    - message: SMS content

    Returns:
    - Message SID if successful
    """
    if not twilio_client:
        logger.warning("Twilio client not initialized. Check Twilio credentials in secrets.")
        raise ValueError("Twilio client not initialized")

    try:
        # Ensure phone number is in E.164 format
        if not phone_number.startswith('+'):
            phone_number = f"+{phone_number}"

        # Send SMS
        twilio_message = twilio_client.messages.create(
            body=message,
            from_=TWILIO_PHONE_NUMBER,
            to=phone_number
        )

        logger.info(f"SMS sent via Twilio: {twilio_message.sid}")
        return twilio_message.sid
    except Exception as e:
        logger.error(f"Failed to send SMS via Twilio: {str(e)}", exc_info=True)
        raise


# Function to send email using AWS SES
def send_email_via_ses(email, subject, html_content, sender=None):
    """
    Send email using AWS SES

    Parameters:
    - email: Recipient's email address
    - subject: Email subject
    - html_content: HTML content of the email
    - sender: Optional sender email address (defaults to no-reply@tidyzon.com)

    Returns:
    - Message ID if successful
    """
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


# Function to hash passwords securely
def hash_password(password, salt=None):
    """Hash a password using SHA-256 with salt"""
    if salt is None:
        salt = os.urandom(32)  # 32 bytes for the salt

    # Combine password and salt
    password_bytes = password.encode('utf-8')
    salted_password = password_bytes + salt

    # Hash the salted password
    hash_obj = hashlib.sha256(salted_password)
    password_hash = hash_obj.hexdigest()

    return password_hash, salt


# Function to verify password
def verify_password(stored_password_hash, stored_salt, provided_password):
    """Verify a password against its stored hash and salt"""
    # Hash the provided password with the stored salt
    provided_hash, _ = hash_password(provided_password, stored_salt)

    # Compare the hashes
    return provided_hash == stored_password_hash


# Function to handle database pagination
def paginate_query_results(cursor, query, params, page=1, page_size=10):
    """
    Execute a query with pagination

    Parameters:
    - cursor: Database cursor
    - query: SQL query without LIMIT/OFFSET
    - params: Query parameters
    - page: Page number (starting from 1)
    - page_size: Number of items per page

    Returns:
    - Dictionary with pagination details and results
    """
    try:
        # Validate pagination params
        page = max(1, int(page))
        page_size = max(1, min(100, int(page_size)))  # Limit maximum page size

        # Calculate offset
        offset = (page - 1) * page_size

        # Add pagination to query
        paginated_query = f"{query} LIMIT %s OFFSET %s"
        paginated_params = list(params) + [page_size, offset]

        # Execute query with pagination
        cursor.execute(paginated_query, paginated_params)
        results = cursor.fetchall()

        # Get total count for pagination metadata
        count_query = f"SELECT COUNT(*) as total FROM ({query}) AS subquery"
        cursor.execute(count_query, params)
        total = cursor.fetchone()['total']

        # Calculate pagination metadata
        total_pages = (total + page_size - 1) // page_size  # Ceiling division
        has_next = page < total_pages
        has_prev = page > 1

        return {
            'results': results,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total_items': total,
                'total_pages': total_pages,
                'has_next': has_next,
                'has_prev': has_prev,
                'next_page': page + 1 if has_next else None,
                'prev_page': page - 1 if has_prev else None
            }
        }
    except Exception as e:
        logger.error(f"Pagination error: {str(e)}", exc_info=True)
        raise


# Function to upload file to S3
def upload_file_to_s3(file_data, bucket, key, content_type=None, metadata=None):
    """
    Upload a file to S3

    Parameters:
    - file_data: Binary data of the file
    - bucket: S3 bucket name
    - key: S3 object key (path)
    - content_type: Optional MIME type
    - metadata: Optional dictionary of metadata

    Returns:
    - S3 URL
    """
    try:
        params = {
            'Bucket': bucket,
            'Key': key,
            'Body': file_data
        }

        if content_type:
            params['ContentType'] = content_type

        if metadata:
            params['Metadata'] = metadata

        s3_client.put_object(**params)

        # Return S3 URL
        return f"s3://{bucket}/{key}"
    except Exception as e:
        logger.error(f"Failed to upload file to S3: {str(e)}", exc_info=True)
        raise


# Function to generate a pre-signed URL for S3 object
def generate_presigned_url(bucket, key, expiry_seconds=3600):
    """
    Generate a pre-signed URL for an S3 object

    Parameters:
    - bucket: S3 bucket name
    - key: S3 object key (path)
    - expiry_seconds: URL expiration time in seconds (default: 1 hour)

    Returns:
    - Pre-signed URL
    """
    try:
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket,
                'Key': key
            },
            ExpiresIn=expiry_seconds
        )

        return presigned_url
    except Exception as e:
        logger.error(f"Failed to generate pre-signed URL: {str(e)}", exc_info=True)
        raise


# Function to format datetime for display
def format_datetime_display(dt):
    """
    Format datetime object for display

    Parameters:
    - dt: Datetime object or ISO string

    Returns:
    - Formatted datetime string
    """
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
        except:
            return dt

    if isinstance(dt, datetime):
        return dt.strftime("%A, %B %d, %Y at %I:%M %p")

    return str(dt)


# Function to get client IP address from event
def get_client_ip(event):
    """
    Extract client IP address from Lambda event

    Parameters:
    - event: Lambda event object

    Returns:
    - Client IP address string
    """
    return event.get('requestContext', {}).get('identity', {}).get('sourceIp', 'unknown')


# Function to get user agent from event
def get_user_agent(event):
    """
    Extract user agent from Lambda event

    Parameters:
    - event: Lambda event object

    Returns:
    - User agent string
    """
    return event.get('requestContext', {}).get('identity', {}).get('userAgent', 'unknown')


# Function to sanitize input
def sanitize_input(input_string):
    """
    Sanitize user input to prevent injection attacks

    Parameters:
    - input_string: String to sanitize

    Returns:
    - Sanitized string
    """
    if not input_string:
        return input_string

    # Remove any HTML tags
    input_string = re.sub(r'<[^>]*>', '', input_string)

    # Escape special characters
    input_string = input_string.replace("'", "''")

    return input_string


# Function to get user information by ID
def get_user_by_id(cursor, user_id):
    """
    Get user information by user ID

    Parameters:
    - cursor: Database cursor
    - user_id: User ID

    Returns:
    - User information dictionary or None if not found
    """
    try:
        cursor.execute("""
            SELECT u.*, ud.*
            FROM users u
            LEFT JOIN userdetails ud ON u.userid = ud.userid
            WHERE u.userid = %s
        """, (user_id,))

        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Failed to get user by ID: {str(e)}", exc_info=True)
        return None


# Function to get user information by email
def get_user_by_email(cursor, email):
    """
    Get user information by email

    Parameters:
    - cursor: Database cursor
    - email: User email

    Returns:
    - User information dictionary or None if not found
    """
    try:
        cursor.execute("""
            SELECT u.*, ud.*
            FROM users u
            LEFT JOIN userdetails ud ON u.userid = ud.userid
            WHERE u.email = %s
        """, (email,))

        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Failed to get user by email: {str(e)}", exc_info=True)
        return None


# Function to log user activity
def log_user_activity(cursor, user_id, activity_type, details, ip_address=None):
    """
    Log user activity to the database

    Parameters:
    - cursor: Database cursor
    - user_id: User ID
    - activity_type: Type of activity
    - details: Dictionary of activity details
    - ip_address: Optional client IP address

    Returns:
    - True if successful, False if failed
    """
    try:
        cursor.execute("""
            INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
            VALUES (%s, %s, %s, %s, NOW())
        """, (
            user_id,
            activity_type,
            json.dumps(details),
            ip_address
        ))

        return True
    except Exception as e:
        logger.error(f"Failed to log user activity: {str(e)}", exc_info=True)
        return False


# Function to create user notification
def create_user_notification(cursor, user_id, notification_type, message):
    """
    Create a notification for a user

    Parameters:
    - cursor: Database cursor
    - user_id: User ID
    - notification_type: Type of notification
    - message: Notification message

    Returns:
    - True if successful, False if failed
    """
    try:
        cursor.execute("""
            INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
            VALUES (%s, %s, %s, %s, NOW())
        """, (
            user_id,
            notification_type,
            message,
            False
        ))

        return True
    except Exception as e:
        logger.error(f"Failed to create user notification: {str(e)}", exc_info=True)
        return False