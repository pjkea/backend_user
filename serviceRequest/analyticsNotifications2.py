import json
import boto3
import logging
# import psycopg2
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, send_email_via_ses, send_sms_via_twilio, log_to_sns

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Load secrets from AWS Secrets Manager
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]

# Initialize cursor
conn = get_db_connection()
cursor = conn.cursor(cursor_factory=RealDictCursor)


def notify_provider_about_review(provider_id, feedback_record, service_details):
    """Notifies a service provider about a new review/feedback via email and SMS."""
    # Fetch provider contact information
    cursor.execute("""SELECT t.userid, u.email, ud.phonenumber FROM tidysp t 
    JOIN users u ON t.userid = u.userid JOIN userdetails ud ON t.userid = ud.userid 
    WHERE tidyspid = %s""", (provider_id,))
    result = cursor.fetchone()

    if not result:
        raise ValueError(f"Provider with ID {provider_id} not found")

    email = result["email"]
    phone_number = result["phonenumber"]

    # Extract feedback details
    rating = feedback_record.get('rating', 'N/A')
    feedback_text = feedback_record.get('comment', 'No comment provided')
    service_id = service_details.get('service_id', 'N/A')

    # Prepare notification content
    email_subject = f"New {rating}-star Feedback Received"

    email_message = f'''<h1>New Feedback Received</h1>
                      <p>You received a <strong>{rating}-star</strong> rating for service ID: {service_id}</p>
                      <p>Customer comments: "{feedback_text}"</p>
                      <a href="https://dashboard.yourservice.com/feedback/{feedback_record.get('feedbackId')}">
                        View details in your dashboard
                      </a>'''

    sms_message = f"New {rating}-star feedback received for service #{service_id}. Log in to your dashboard to view details."

    notification_status = {"email": None, "sms": None}

    # Send notifications
    try:
        notification_status["email"] = send_email_via_ses(email, email_subject, email_message)
        logger.info(f"Email notification sent to provider {provider_id}")
    except Exception as e:
        logger.error(f"Failed to send email notification to provider {provider_id}: {e}")
        notification_status["email"] = f"Failed: {str(e)}"

    try:
        notification_status["sms"] = send_sms_via_twilio(phone_number, sms_message)
        logger.info(f"SMS notification sent to provider {provider_id}")
    except Exception as e:
        logger.error(f"Failed to send SMS notification to provider {provider_id}: {e}")
        notification_status["sms"] = f"Failed: {str(e)}"

    return notification_status


def update_rating_summary(service_id, provider_id, new_rating):
    """Update the running average rating for service and provider"""
    try:
        # Check for existing reviews for this provider
        cursor.execute("""
            SELECT COUNT(*) as review_count, AVG(rating) as avg_rating 
            FROM reviews 
            WHERE tidyspid = %s
        """, (provider_id,))

        provider_stats = cursor.fetchone()

        result = {
            "provider_id": provider_id,
            "service_id": service_id,
            "new_rating": new_rating,
            "provider_rating": None,
            "service_rating": None,
            "is_first_provider_review": False,
            "is_first_service_review": False
        }

        # Calculate and update provider overall rating
        if provider_stats["review_count"] == 0:
            # This is the first review for the provider
            result["provider_rating"] = new_rating
            result["is_first_provider_review"] = True

            # Insert into tidyspmetrics for overall provider rating
            cursor.execute("""
                INSERT INTO tidyspmetrics (tidyspid, metricname, tableid, createdat) 
                VALUES (%s, %s, %s, NOW())
            """, (provider_id, "AvgRating", provider_id))

            metric_id = cursor.lastrowid

            # Add rating value to the associated metrics value table
            cursor.execute("""
                INSERT INTO tidyspmetric_values (tidyspmetricid, value, createdat) 
                VALUES (%s, %s, NOW())
            """, (metric_id, new_rating))

        else:
            # Calculate new average including the new rating
            total_reviews = provider_stats["review_count"] + 1
            total_rating_sum = (provider_stats["avg_rating"] * provider_stats["review_count"]) + float(new_rating)
            new_avg_rating = total_rating_sum / total_reviews
            result["provider_rating"] = round(new_avg_rating, 2)

            # Check if metric already exists
            cursor.execute("""
                SELECT tidyspmetricid FROM tidyspmetrics 
                WHERE tidyspid = %s AND metricname = %s
            """, (provider_id, "AvgRating"))

            metric_record = cursor.fetchone()

            if metric_record:
                metric_id = metric_record["tidyspmetricid"]

                # Update the metric value
                cursor.execute("""
                    UPDATE tidyspmetric_values 
                    SET value = %s, updatedat = NOW() 
                    WHERE tidyspmetricid = %s
                """, (result["provider_rating"], metric_id))
            else:
                # Create new metric if it doesn't exist
                cursor.execute("""
                    INSERT INTO tidyspmetrics (tidyspid, metricname, tableid, createdat) 
                    VALUES (%s, %s, %s, NOW())
                """, (provider_id, "AvgRating", provider_id))

                metric_id = cursor.lastrowid

                # Add rating value
                cursor.execute("""
                    INSERT INTO tidyspmetric_values (tidyspetricid, value, createdat) 
                    VALUES (%s, %s, NOW())
                """, (metric_id, result["provider_rating"]))

        # Commit the transaction
        conn.commit()

        logger.info(f"Updated rating summary for provider {provider_id}, service {service_id}")
        return result

    except Exception as e:
        # Roll back in case of error
        conn.rollback()
        logger.error(f"Failed to update rating summary: {e}")
        raise


def lambda_handler(event, context):
    try:
        logger.info(f"Lambda 2 - Received feedback event: {json.dumps(event)}")

        for record in event.get("Records", []):
            message = json.loads(record['Sns']['Message'])
            feedback_record = message.get('feedbackRecord', {})
            service_details = message.get('serviceDetails', {})
            actions = message.get('actions', {})

            order_id = feedback_record.get('orderid')
            user_id = feedback_record.get('userid')
            tidysp_id = feedback_record.get('tidyspid')
            rating = feedback_record.get('rating')

            results = {
                'providerNotified': False,
                'ratingSummaryUpdated': False,
            }

            if actions.get('notifyProvider'):
                notify_provider_about_review(tidysp_id, feedback_record, service_details)
                results['providerNotified'] = True

            if actions.get('updateRatingSummary'):
                update_rating_summary(order_id, tidysp_id, rating)
                results['ratingSummaryUpdated'] = True

            # Log to SNS
            log_to_sns(1, 6,12, 1, {'orderid': order_id, 'tidyspid': tidysp_id}, 'NotifyProvider', user_id)

            logger.info(f"Async processing results for feedback {order_id}: {results}")

            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Feedback processing completed successfully'})
            }

    except Exception as e:
        logger.error(f"Lambda 2 - Error in async feedback processing: {str(e)}")

        # Log to SNS
        log_to_sns(4,6,12,43,{'orderid': order_id, 'tidyspid': tidysp_id}, 'NotifyProvider - Error', user_id)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Feedback async processing completed with errors',
                'error': str(e)
            })
        }

    finally:
        cursor.close()
        conn.close()








