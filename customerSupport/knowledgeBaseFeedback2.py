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
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
KB_ANALYTICS_TOPIC_ARN = secrets.get("KB_ANALYTICS_TOPIC_ARN")


def update_article_metrics(article_id, helpful, rating, cursor):
    """Update article metrics based on feedback"""
    try:
        # Get current metrics
        cursor.execute("""
            SELECT views, helpful_count, not_helpful_count, 
                   rating_count, rating_sum, avg_rating
            FROM kb_article_metrics
            WHERE article_id = %s
        """, (article_id,))

        metrics = cursor.fetchone()

        if metrics:
            logger.info(f"Article metrics found for article ID {article_id}")

            # Update metrics
            updates = []
            values = []

            if helpful is not None:
                if helpful:
                    updates.append("helpful_count = helpful_count + 1")
                else:
                    updates.append("not_helpful_count = not_helpful_count + 1")

            if rating is not None:
                # Update rating metrics
                new_rating_count = metrics['rating_count'] + 1
                new_rating_sum = metrics['rating_sum'] + rating
                new_avg_rating = new_rating_sum / new_rating_count

                updates.extend([
                    "rating_count = %s",
                    "rating_sum = %s",
                    "avg_rating = %s"
                ])
                values.extend([new_rating_count, new_rating_sum, new_avg_rating])

            # Update last updated timestamp
            updates.append("last_updated = %s")
            values.append(datetime.now())

            # Prepare SQL update statement
            sql = f"""
                UPDATE kb_article_metrics
                SET {', '.join(updates)}
                WHERE article_id = %s
            """
            values.append(article_id)

            # Execute update
            cursor.execute(sql, values)

            logger.info(f"Article metrics updated for article ID {article_id}")
        else:
            logger.info(f"No metrics found, creating new metrics for article ID {article_id}")

            # Initialize new metrics record
            helpful_count = 1 if helpful else 0
            not_helpful_count = 0 if helpful else 1
            rating_count = 1 if rating is not None else 0
            rating_sum = rating if rating is not None else 0
            avg_rating = rating if rating is not None else 0

            cursor.execute("""
                INSERT INTO kb_article_metrics
                (article_id, views, helpful_count, not_helpful_count, 
                 rating_count, rating_sum, avg_rating, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                article_id,
                0,  # Initial views count
                helpful_count,
                not_helpful_count,
                rating_count,
                rating_sum,
                avg_rating,
                datetime.now()
            ))

            logger.info(f"New article metrics created for article ID {article_id}")

        return True

    except Exception as e:
        logger.error(f"Error updating article metrics: {str(e)}")
        return False


def check_for_negative_trends(article_id, cursor):
    """Check for negative feedback trends that might require attention"""
    try:
        # Check for low ratings trend
        cursor.execute("""
            SELECT COUNT(*) as low_ratings
            FROM kb_article_feedback
            WHERE article_id = %s AND rating <= 2
            AND created_at > NOW() - INTERVAL '7 days'
        """, (article_id,))

        low_ratings_result = cursor.fetchone()

        if low_ratings_result and low_ratings_result['low_ratings'] >= 3:
            logger.warning(
                f"Article ID {article_id} has received {low_ratings_result['low_ratings']} low ratings in the past week")
            return True, f"Multiple low ratings ({low_ratings_result['low_ratings']}) in the past week"

        # Check for negative helpful/not helpful ratio
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN is_helpful = TRUE THEN 1 ELSE 0 END) as helpful_count,
                SUM(CASE WHEN is_helpful = FALSE THEN 1 ELSE 0 END) as not_helpful_count
            FROM kb_article_feedback
            WHERE article_id = %s
            AND created_at > NOW() - INTERVAL '30 days'
        """, (article_id,))

        helpfulness_result = cursor.fetchone()

        if helpfulness_result:
            helpful_count = helpfulness_result['helpful_count'] or 0
            not_helpful_count = helpfulness_result['not_helpful_count'] or 0

            # If we have sufficient feedback and most of it is negative
            if (helpful_count + not_helpful_count >= 5) and (not_helpful_count > helpful_count * 2):
                logger.warning(
                    f"Article ID {article_id} has poor helpfulness ratio: {helpful_count} helpful vs {not_helpful_count} not helpful")
                return True, f"Poor helpfulness ratio ({helpful_count} helpful vs {not_helpful_count} not helpful)"

        return False, None

    except Exception as e:
        logger.error(f"Error checking for negative trends: {str(e)}")
        return False, None


def create_content_review_task(article_id, article_title, reason, cursor):
    """Create a content review task for the knowledge base team"""
    try:
        cursor.execute("""
            INSERT INTO kb_content_reviews
            (article_id, review_reason, status, created_at)
            VALUES (%s, %s, %s, %s)
            RETURNING review_id
        """, (
            article_id,
            reason,
            'Pending',
            datetime.now()
        ))

        result = cursor.fetchone()

        if result:
            logger.info(f"Content review task created for article ID {article_id}, review ID {result['review_id']}")

            # Create a notification entry for the knowledge base team
            cursor.execute("""
                INSERT INTO system_notifications
                (notification_type, reference_id, reference_type, title, message, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                'kb_review',
                result['review_id'],
                'kb_content_review',
                f"Content Review Required: {article_title}",
                f"Article has received negative feedback. Reason: {reason}",
                datetime.now()
            ))

            logger.info(f"Notification created for content review of article ID {article_id}")
            return True
        else:
            logger.warning(f"Failed to create content review task for article ID {article_id}")
            return False

    except Exception as e:
        logger.error(f"Error creating content review task: {str(e)}")
        return False


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

                # Extract data
                feedback_id = message.get('feedback_id')
                article_id = message.get('article_id')
                article_title = message.get('article_title')
                category_id = message.get('category_id')
                user_id = message.get('user_id')
                helpful = message.get('helpful')
                rating = message.get('rating')
                comment = message.get('comment')

                if not article_id:
                    raise ValueError("Missing required article information")

                # Update article metrics
                metrics_updated = update_article_metrics(article_id, helpful, rating, cursor)

                # Check for negative trends
                needs_review, review_reason = check_for_negative_trends(article_id, cursor)

                # Create content review task if needed
                review_created = False
                if needs_review:
                    review_created = create_content_review_task(article_id, article_title, review_reason, cursor)

                # Send to analytics if analytics topic configured
                if KB_ANALYTICS_TOPIC_ARN:
                    analytics_data = {
                        'feedback_id': feedback_id,
                        'article_id': article_id,
                        'category_id': category_id,
                        'user_id': user_id,
                        'helpful': helpful,
                        'rating': rating,
                        'comment': comment,
                        'timestamp': datetime.now().isoformat(),
                        'event_type': 'kb_feedback'
                    }

                    sns_client.publish(
                        TopicArn=KB_ANALYTICS_TOPIC_ARN,
                        Message=json.dumps(analytics_data),
                        Subject="KB Analytics: Article Feedback"
                    )

                    logger.info(f"Analytics data sent for article feedback on article ID {article_id}")

                # Commit changes
                connection.commit()

                # Log success
                record_result['success'] = True
                record_result['article_id'] = article_id
                record_result['metrics_updated'] = metrics_updated
                record_result['needs_review'] = needs_review
                if needs_review:
                    record_result['review_reason'] = review_reason
                    record_result['review_created'] = review_created

                logger.info(f"Successfully processed feedback for article ID {article_id}")

            except Exception as record_error:
                logger.error(f"Error processing article feedback: {str(record_error)}")
                record_result['error'] = str(record_error)

                # Roll back transaction for this record
                connection.rollback()

                # Log error
                log_to_sns(4, 30, 11, 43, record_result, "KB Article Feedback Processing Error",
                           user_id if 'user_id' in locals() else None)

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
                'message': 'Failed to process article feedback',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()