import json
import boto3
import logging
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

# S3 bucket for storing photos
PHOTOS_BUCKET = secrets["PHOTOS_BUCKET"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")
        order_id = query_params.get("orderid")
        user_type = query_params.get("usertype", "user")  # Default to regular user if not specified

        # Validate required parameters
        if not user_id or not order_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameters: userid and orderid'})
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # First verify that the user has access to this order
        if user_type.lower() == "tidysp":
            cursor.execute("""
                SELECT o.orderid, o.status, o.userid, o.tidyspid, o.completedat
                FROM orders o 
                WHERE o.orderid = %s AND o.tidyspid = %s
            """, (order_id, user_id))
        else:
            cursor.execute("""
                SELECT o.orderid, o.status, o.userid, o.tidyspid, o.completedat
                FROM orders o 
                WHERE o.orderid = %s AND o.userid = %s
            """, (order_id, user_id))

        order = cursor.fetchone()
        if not order:
            return {
                'statusCode': 403,
                'body': json.dumps({'message': 'Unauthorized access to this order'})
            }

        # Retrieve before-and-after photos
        cursor.execute("""
            SELECT photoid, orderid, phototype, photourl, createdat, caption
            FROM servicephotos
            WHERE orderid = %s
            ORDER BY phototype, createdat
        """, (order_id,))

        photos = cursor.fetchall()

        # Format photos for response
        before_photos = []
        after_photos = []

        for photo in photos:
            formatted_photo = dict(photo)

            # Convert datetime to ISO string
            if isinstance(formatted_photo.get('createdat'), datetime):
                formatted_photo['createdat'] = formatted_photo['createdat'].isoformat()

            # Generate pre-signed URL if URL is an S3 path
            if formatted_photo.get('photourl') and formatted_photo['photourl'].startswith('s3://'):
                # Extract bucket and key from S3 URL
                s3_path = formatted_photo['photourl'].replace('s3://', '', 1)
                parts = s3_path.split('/', 1)

                if len(parts) == 2:
                    bucket = parts[0]
                    key = parts[1]

                    # Generate pre-signed URL (valid for 1 hour)
                    try:
                        presigned_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={
                                'Bucket': bucket,
                                'Key': key
                            },
                            ExpiresIn=3600  # 1 hour in seconds
                        )
                        formatted_photo['presigned_url'] = presigned_url
                    except Exception as e:
                        logger.warning(f"Failed to generate presigned URL for {formatted_photo['photourl']}: {e}")

            # Categorize photos
            if formatted_photo.get('phototype', '').upper() == 'BEFORE':
                before_photos.append(formatted_photo)
            elif formatted_photo.get('phototype', '').upper() == 'AFTER':
                after_photos.append(formatted_photo)

        # Log success to SNS
        log_to_sns(1, 11, 5, 1, {
            "user_id": user_id,
            "order_id": order_id,
            "before_photos_count": len(before_photos),
            "after_photos_count": len(after_photos)
        }, "View Before-After Photos - Success", user_id)

        logger.info(f"Successfully retrieved photos for order {order_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Photos retrieved successfully',
                'order_id': order_id,
                'before_photos': before_photos,
                'after_photos': after_photos,
                'total_photos': len(before_photos) + len(after_photos)
            })
        }

    except Exception as e:
        logger.error(f"Failed to retrieve photos: {e}")

        # Log error to SNS
        log_to_sns(4, 11, 5, 43, {
            "user_id": user_id,
            "order_id": order_id,
            "error": str(e)
        }, "View Before-After Photos - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to retrieve photos',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()