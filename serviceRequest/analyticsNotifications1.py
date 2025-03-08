import json
import datetime
import boto3
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Load secrets from AWS Secrets Manager
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
FEEDBACK_ANALYTICS_TOPIC_ARN = secrets["FEEDBACK_ANALYTICS_TOPIC_ARN"]


def lambda_handler(event, context):
    conn = None
    cursor = None
    try:
        logger.info(f"Lambda 1 - Received feedback event: {json.dumps(event)}")

        body = json.loads(event.get("body", "{}"))

        order_id = body.get("order_id")
        user_id = body.get("user_id")
        rating = body.get("rating")
        feedback_text = body.get("feedback_text", '')
        submission_timestamp = body.get("timestamp", datetime.datetime.now().isoformat())
        service_details = {}

        if not order_id or not user_id or rating is None:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required fields: serviceId, userId, and rating are required'
                })
            }

        rating_value = float(rating)
        if rating_value < 1 or rating_value > 5:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Rating must be between 1 and 5'})
            }

        # Retrieve order details
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""SELECT o.tidyspid, o.totalprice, od.add_ons FROM  orders o 
        JOIN orderdetails od ON o.orderid = od.orderid WHERE o.orderid = %s""", (order_id,))

        service_details = cursor.fetchone()
        tidyspid = service_details['tidyspid']

        feedback_record = {
            'orderid': order_id,
            'userid': user_id,
            'tidyspid': tidyspid,
            'rating': rating_value,
            'feedbackText': feedback_text,
            'submissiontimestamp': submission_timestamp,
            'status': 'RECEIVED'
        }

        message = {
            'feedbackRecord': feedback_record,
            'serviceDetails': service_details,
            'actions': {
                'updateAnalytics': True,
                'notifyProvider': tidyspid is not None,
                'updateRatingSummary': True,
            }
        }

        # Publish to SNS
        sns_client.publish(
            TopicArn=FEEDBACK_ANALYTICS_TOPIC_ARN,
            Message=json.dumps(message),
        )
        logger.info("Successfully sent feedback")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Thank you for your feedback',
                'orderid': order_id,
                'user_id': user_id,
                'timestamp': submission_timestamp
            })
        }

    except Exception as e:
        logger.error(f"Lambda 1 - Error processing feedback submission: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error submitting feedback',
                'error': str(e)
            })
        }


