import json
import boto3
import psycopg2
import logging
import datetime
import uuid

from psycopg2.extras import DictCursor

from layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")

# Load secrets from AWS Secrets Manager
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    conn = None
    cursor = None
    try:
        logger.info(f"Received feedback event: {json.dumps(event)}")

        body = json.loads(event.get("body", "{}"))

        order_id = body.get("order_id")
        user_id = body.get("user_id")
        rating = body.get("rating")
        feedback_text = body.get("feedback_text", '')
        submission_timestamp = body.get("timestamp", datetime.datetime.now().isoformat())

        if not order_id or not user_id or rating is None:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required fields: serviceId, userId, and rating are required'})
            }

        rating_value = float(rating)
        if rating_value < 1 or rating_value > 5:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Rating must be between 1 and 5'})
            }

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        # Retrieve tidysp info
        cursor.execute("""SELECT tidyspid FROM orders WHERE orderid = %s""", (order_id,))
        tidyspid = cursor.fetchone()

        # Insert into reviews table
        cursor.execute("""INSERT INTO reviews (orderid, userid, tidyspid, rating, comments, createdat)
        VALUES (%s, %s, %s, %s, %s, NOW())""", (order_id, user_id, tidyspid, rating_value, feedback_text))

        conn.commit()

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Thank you for your feedback',
                'order_id': order_id,
                'user_id': user_id,
                'rating': rating_value,
                'feedback_text': feedback_text,
                'timestamp': submission_timestamp,
            })
        }

    except Exception as e:
        logger.error(f"Error processing feedback submission: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error submitting feedback',
                'error': str(e)
            })
        }


