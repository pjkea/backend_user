import json
import boto3
import logging
import base64
import re
import uuid
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')
s3_client = boto3.client('s3', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
PROFILE_PICTURE_TOPIC_ARN = secrets["PROFILE_PICTURE_TOPIC_ARN"]

# S3 bucket for storing profile pictures
PROFILE_PICTURES_BUCKET = secrets["PROFILE_PICTURES_BUCKET"]

# Max file size (5MB)
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB in bytes

# Allowed image types
ALLOWED_CONTENT_TYPES = [
    'image/jpeg',
    'image/jpg',
    'image/png',
    'image/gif'
]


def validate_image_data(base64_data, content_type):
    """Validate the image data and content type"""
    # Check content type
    if content_type not in ALLOWED_CONTENT_TYPES:
        return False, f"Invalid content type. Allowed types: {', '.join(ALLOWED_CONTENT_TYPES)}"

    # Check if the base64 data is valid
    try:
        # Decode base64 data
        if ';base64,' in base64_data:
            # Extract the actual base64 part if it's in a data URL format
            base64_data = base64_data.split(';base64,')[1]

        image_data = base64.b64decode(base64_data)

        # Check file size
        if len(image_data) > MAX_FILE_SIZE:
            return False, f"Image size exceeds maximum allowed size of {MAX_FILE_SIZE / (1024 * 1024)}MB"

        return True, image_data
    except Exception as e:
        return False, f"Invalid base64 data: {str(e)}"


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")

        body = json.loads(event.get('body', '{}'))
        image_data = body.get('image_data')  # Base64 encoded image
        content_type = body.get('content_type')

        # Validate required parameters
        if not user_id or not image_data or not content_type:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: userid, image_data, and content_type are required'
                })
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify user exists
        cursor.execute("""
            SELECT userid FROM users WHERE userid = %s
        """, (user_id,))

        if not cursor.fetchone():
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'User not found'})
            }

        # Validate image data
        is_valid, result = validate_image_data(image_data, content_type)
        if not is_valid:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': result})
            }

        # At this point, result contains the decoded image data
        image_bytes = result

        # Generate a unique filename for the image
        file_extension = content_type.split('/')[-1]
        if file_extension == 'jpeg':
            file_extension = 'jpg'

        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        filename = f"profile_{user_id}_{timestamp}_{uuid.uuid4().hex[:8]}.{file_extension}"

        # Upload image to S3
        s3_key = f"profile_pictures/{user_id}/{filename}"

        s3_client.put_object(
            Bucket=PROFILE_PICTURES_BUCKET,
            Key=s3_key,
            Body=image_bytes,
            ContentType=content_type,
            Metadata={
                'user_id': str(user_id),
                'upload_timestamp': timestamp
            }
        )

        # Create S3 URL for the image
        s3_url = f"s3://{PROFILE_PICTURES_BUCKET}/{s3_key}"

        # Create a pre-signed URL for immediate viewing
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': PROFILE_PICTURES_BUCKET,
                'Key': s3_key
            },
            ExpiresIn=3600  # 1 hour
        )

        # Prepare message for SNS
        message = {
            'user_id': user_id,
            's3_url': s3_url,
            'content_type': content_type,
            'file_size': len(image_bytes),
            'original_filename': filename,
            's3_key': s3_key,
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=PROFILE_PICTURE_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'Profile Picture Upload: {user_id}'
        )

        # Log success to SNS
        log_to_sns(1, 7, 4, 1, {
            "user_id": user_id,
            "file_size": len(image_bytes),
            "content_type": content_type,
            "s3_url": s3_url
        }, "Profile Picture Upload - Success", user_id)

        logger.info(f"Successfully uploaded profile picture for user {user_id}")

        return {
            'statusCode': 202,  # Accepted
            'body': json.dumps({
                'message': 'Profile picture uploaded successfully and is being processed',
                'temporary_url': presigned_url,
                's3_url': s3_url,
                'note': 'Your profile picture will be optimized and updated shortly'
            })
        }

    except Exception as e:
        logger.error(f"Failed to upload profile picture: {e}")

        # Log error to SNS
        log_to_sns(4, 7, 4, 43, {
            "user_id": user_id,
            "error": str(e)
        }, "Profile Picture Upload - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to upload profile picture',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()