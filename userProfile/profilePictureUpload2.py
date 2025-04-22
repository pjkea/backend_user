import json
import boto3
import logging
import io
import os
from datetime import datetime
from PIL import Image
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, send_email_via_ses

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

# S3 bucket for storing profile pictures
PROFILE_PICTURES_BUCKET = secrets["PROFILE_PICTURES_BUCKET"]

# Image sizes for optimization
PROFILE_IMAGE_SIZES = {
    'large': (500, 500),
    'medium': (200, 200),
    'thumbnail': (100, 100)
}


def optimize_image(image_data, content_type):
    """Optimize and resize the image for different use cases"""
    try:
        # Open the image with PIL
        img = Image.open(io.BytesIO(image_data))

        # Convert PNG with alpha channel to RGB with white background
        if img.mode == 'RGBA':
            background = Image.new('RGB', img.size, (255, 255, 255))
            background.paste(img, mask=img.split()[3])  # Use alpha channel as mask
            img = background

        # Prepare result dictionary with optimized images
        result = {}

        # Create different sizes
        for size_name, dimensions in PROFILE_IMAGE_SIZES.items():
            # Create a copy of the image and resize it
            img_copy = img.copy()
            img_copy.thumbnail(dimensions, Image.LANCZOS)

            # Convert to RGB mode if not already
            if img_copy.mode != 'RGB' and content_type != 'image/png':
                img_copy = img_copy.convert('RGB')

            # Save to bytes
            output = io.BytesIO()

            # Determine format based on content type
            img_format = 'JPEG'
            if content_type == 'image/png':
                img_format = 'PNG'
            elif content_type == 'image/gif':
                img_format = 'GIF'

            # Save with appropriate quality
            if img_format == 'JPEG':
                img_copy.save(output, format=img_format, quality=85, optimize=True)
            else:
                img_copy.save(output, format=img_format, optimize=True)

            output.seek(0)
            result[size_name] = output.getvalue()

        return True, result
    except Exception as e:
        logger.error(f"Image optimization failed: {e}")
        return False, str(e)


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Process SNS records
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            # Extract data from message
            user_id = message.get('user_id')
            s3_url = message.get('s3_url')
            content_type = message.get('content_type')
            s3_key = message.get('s3_key')

            # Download the image from S3
            response = s3_client.get_object(
                Bucket=PROFILE_PICTURES_BUCKET,
                Key=s3_key
            )

            image_data = response['Body'].read()

            # Optimize the image
            is_successful, result = optimize_image(image_data, content_type)

            if not is_successful:
                logger.error(f"Failed to optimize image: {result}")
                continue

            # At this point, result contains resized images
            optimized_images = result

            # Upload optimized images to S3
            original_filename = os.path.basename(s3_key)
            filename_parts = os.path.splitext(original_filename)
            base_path = os.path.dirname(s3_key)

            # Store URLs for different sizes
            image_urls = {}

            for size_name, image_bytes in optimized_images.items():
                # Create new filename with size suffix
                new_filename = f"{filename_parts[0]}_{size_name}{filename_parts[1]}"
                new_s3_key = f"{base_path}/{new_filename}"

                # Upload to S3
                s3_client.put_object(
                    Bucket=PROFILE_PICTURES_BUCKET,
                    Key=new_s3_key,
                    Body=image_bytes,
                    ContentType=content_type,
                    Metadata={
                        'user_id': str(user_id),
                        'size': size_name,
                        'original_key': s3_key
                    }
                )

                # Store S3 URL
                image_urls[size_name] = f"s3://{PROFILE_PICTURES_BUCKET}/{new_s3_key}"

                # Generate a pre-signed URL for each image size
                presigned_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': PROFILE_PICTURES_BUCKET,
                        'Key': new_s3_key
                    },
                    ExpiresIn=3600 * 24  # 24 hours
                )

                image_urls[f"{size_name}_url"] = presigned_url

            # Update the user's profile picture in the database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Get user information
            cursor.execute("""
                SELECT u.email, ud.userid, ud.profilepicture
                FROM users u
                LEFT JOIN userdetails ud ON u.userid = ud.userid
                WHERE u.userid = %s
            """, (user_id,))

            user_info = cursor.fetchone()

            # Default to medium size for the profile picture
            profile_picture_url = image_urls.get('medium', s3_url)

            # Check if userdetails record exists
            if user_info and user_info.get('userid'):
                # Update existing userdetails record
                cursor.execute("""
                    UPDATE userdetails
                    SET profilepicture = %s, updatedat = NOW()
                    WHERE userid = %s
                """, (profile_picture_url, user_id))
            else:
                # Create new userdetails record
                cursor.execute("""
                    INSERT INTO userdetails (userid, profilepicture, createdat)
                    VALUES (%s, %s, NOW())
                """, (user_id, profile_picture_url))

            # Log the profile picture update in activity logs
            cursor.execute("""
                INSERT INTO user_activity_logs (userid, activity_type, details, createdat)
                VALUES (%s, %s, %s, NOW())
            """, (
                user_id,
                'PROFILE_PICTURE_UPDATE',
                json.dumps({
                    'previous_picture': user_info.get('profilepicture') if user_info else None,
                    'new_picture': profile_picture_url,
                    'available_sizes': list(PROFILE_IMAGE_SIZES.keys())
                })
            ))

            connection.commit()

            # Create notification in app
            cursor.execute("""
                INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'PROFILE_UPDATE',
                'Your profile picture has been updated successfully.',
                False
            ))

            # Send email notification if email exists
            if user_info and user_info.get('email'):
                user_email = user_info['email']

                email_subject = "Profile Picture Updated"
                email_message = f"""
                <h2>Profile Picture Updated</h2>
                <p>Your profile picture has been updated successfully.</p>
                <p>You can view your profile to see the changes.</p>
                <p>Thank you for using our service!</p>
                """

                try:
                    send_email_via_ses(user_email, email_subject, email_message)
                    logger.info(f"Profile picture update email sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send email: {e}")

            # Log success to SNS
            log_to_sns(1, 7, 4, 1, {
                "user_id": user_id,
                "profile_picture": profile_picture_url,
                "available_sizes": list(image_urls.keys())
            }, "Profile Picture Processing - Success", user_id)

            logger.info(f"Successfully processed profile picture for user {user_id}")

            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Profile picture processed successfully',
                    'profile_picture': profile_picture_url,
                    'image_urls': image_urls
                })
            }

    except Exception as e:
        logger.error(f"Failed to process profile picture: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 4, 43, {
            "user_id": user_id if 'user_id' in locals() else 'unknown',
            "error": str(e)
        }, "Profile Picture Processing - Failed", user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process profile picture',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()