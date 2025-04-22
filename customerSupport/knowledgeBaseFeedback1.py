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
KB_FEEDBACK_TOPIC_ARN = secrets["KB_FEEDBACK_TOPIC_ARN"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Extract user ID from query parameters
        user_id = event.get('queryStringParameters', {}).get('userid')

        # Parse request body
        body = json.loads(event.get('body', '{}'))
        article_id = body.get('article_id')

        if not article_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: article_id'})
            }

        # Extract feedback data
        helpful = body.get('helpful')  # Boolean
        rating = body.get('rating')  # 1-5 star rating
        comment = body.get('comment')  # Optional comment

        # Validate parameters
        if helpful is None and rating is None:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'At least one feedback parameter (helpful or rating) is required'})
            }

        if rating is not None:
            try:
                rating = int(rating)
                if rating < 1 or rating > 5:
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'message': 'Rating must be between 1 and 5'})
                    }
            except (ValueError, TypeError):
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Invalid rating format'})
                }

        # Create database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify article exists
        cursor.execute("""
            SELECT article_id, title, category_id 
            FROM knowledge_base_articles 
            WHERE article_id = %s AND is_published = TRUE
        """, (article_id,))

        article = cursor.fetchone()

        if article:
            logger.info(f"Article found for article ID {article_id}")
        else:
            logger.warning(f"No article found for article ID {article_id}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Article not found'})
            }

        # Create feedback record
        feedback_data = {
            'article_id': article_id,
            'user_id': user_id,
            'helpful': helpful,
            'rating': rating,
            'comment': comment,
            'timestamp': datetime.now().isoformat()
        }

        cursor.execute("""
            INSERT INTO kb_article_feedback
            (article_id, user_id, is_helpful, rating, comment_text, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING feedback_id
        """, (
            article_id,
            user_id,
            helpful,
            rating,
            comment,
            datetime.now()
        ))

        result = cursor.fetchone()
        if result:
            logger.info(f"Feedback record created for article ID {article_id}")
            feedback_id = result['feedback_id']
        else:
            logger.warning(f"Failed to create feedback record for article ID {article_id}")
            feedback_id = None

        # Prepare message for SNS
        feedback_message = {
            'feedback_id': feedback_id,
            'article_id': article_id,
            'article_title': article.get('title'),
            'category_id': article.get('category_id'),
            'user_id': user_id,
            'helpful': helpful,
            'rating': rating,
            'comment': comment,
            'timestamp': datetime.now().isoformat()
        }

        # Publish to SNS for asynchronous processing
        sns_client.publish(
            TopicArn=KB_FEEDBACK_TOPIC_ARN,
            Message=json.dumps(feedback_message),
            Subject=f"KB Article Feedback: {article_id}"
        )

        # Commit database changes
        connection.commit()

        # Log successful feedback submission
        log_data = {
            'article_id': article_id,
            'feedback_id': feedback_id,
            'helpful': helpful,
            'rating': rating
        }
        log_to_sns(1, 30, 11, 1, log_data, "KB Article Feedback Submitted", user_id)

        logger.info(f"Successfully submitted feedback for article {article_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Thank you for your feedback',
                'feedback_id': feedback_id
            })
        }

    except Exception as e:
        logger.error(f"Error submitting article feedback: {str(e)}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error
        error_data = {
            'article_id': article_id if 'article_id' in locals() else None,
            'user_id': user_id if 'user_id' in locals() else None,
            'error': str(e)
        }
        log_to_sns(4, 30, 11, 43, error_data, "KB Article Feedback Error",
                   user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to submit article feedback',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()