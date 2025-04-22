import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
REVIEW_RATING_TOPIC_ARN = secrets["REVIEW_RATING_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")
        user_type = query_params.get("usertype", "user")  # 'user' or 'tidysp'

        body = json.loads(event.get('body', '{}'))
        order_id = body.get('order_id')
        rating = body.get('rating')
        review_text = body.get('review_text', '')

        # Validate required parameters
        if not user_id or not order_id or rating is None:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters: userid, order_id, and rating are required'
                })
            }

        # Validate rating
        try:
            rating_value = float(rating)
            if rating_value < 1 or rating_value > 5:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'message': 'Rating must be between 1 and 5'
                    })
                }
        except ValueError:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Rating must be a number between 1 and 5'
                })
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify that the user has access to this order
        if user_type.lower() == 'tidysp':
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
                'body': json.dumps({
                    'message': 'Unauthorized access to this order'
                })
            }

        # Check if the order is completed (only completed orders can be rated)
        if order['status'] != 'COMPLETED':
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Order is not completed yet, rating not available',
                    'status': order['status']
                })
            }

        # Check if user has already submitted a rating
        cursor.execute("""
            SELECT reviewid, rating, comments, createdat
            FROM reviews
            WHERE orderid = %s AND userid = %s
        """, (order_id, user_id))

        existing_review = cursor.fetchone()

        if existing_review:
            return {
                'statusCode': 409,  # Conflict
                'body': json.dumps({
                    'message': 'You have already rated this order',
                    'review_id': existing_review['reviewid'],
                    'rating': float(existing_review['rating']),
                    'review_text': existing_review['comments'],
                    'created_at': existing_review['createdat'].isoformat()
                })
            }

        # Insert the review
        cursor.execute("""
            INSERT INTO reviews (orderid, userid, rating, comments, createdat)
            VALUES (%s, %s, %s, %s, NOW())
            RETURNING reviewid
        """, (order_id, user_id, rating_value, review_text))

        review_result = cursor.fetchone()
        review_id = review_result['reviewid']

        # Determine the reviewee (who is being reviewed)
        if user_type.lower() == 'tidysp':
            reviewee_id = order['userid']  # TidySP reviewing customer
            is_customer_review = False
        else:
            reviewee_id = order['tidyspid']  # Customer reviewing TidySP
            is_customer_review = True

        connection.commit()

        # Prepare message for SNS
        message = {
            'review_id': review_id,
            'order_id': order_id,
            'reviewer_id': user_id,
            'reviewer_type': user_type,
            'reviewee_id': reviewee_id,
            'rating': rating_value,
            'review_text': review_text,
            'is_customer_review': is_customer_review,
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS
        sns_client.publish(
            TopicArn=REVIEW_RATING_TOPIC_ARN,
            Message=json.dumps(message),
            Subject=f'New Review: {review_id}'
        )

        # Log success to SNS
        log_to_sns(1, 11, 10, 1, {
            "user_id": user_id,
            "order_id": order_id,
            "review_id": review_id,
            "rating": rating_value
        }, "Submit Review - Success", user_id)

        logger.info(f"Successfully submitted review for order {order_id}")

        return {
            'statusCode': 201,  # Created
            'body': json.dumps({
                'message': 'Review submitted successfully',
                'review_id': review_id,
                'order_id': order_id,
                'rating': rating_value,
                'review_text': review_text,
                'note': 'Thank you for your feedback!'
            })
        }

    except Exception as e:
        logger.error(f"Failed to submit review: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 11, 10, 43, {
            "user_id": user_id,
            "order_id": order_id,
            "error": str(e)
        }, "Submit Review - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to submit review',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()