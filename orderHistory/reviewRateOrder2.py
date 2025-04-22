import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, send_email_via_ses, send_sms_via_twilio

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


def update_rating_summary(cursor, tidysp_id, new_rating):
    """Calculate and update the average rating for a TidySP"""
    # Get current ratings
    cursor.execute("""
        SELECT COUNT(*) as review_count, AVG(rating) as avg_rating 
        FROM reviews 
        WHERE tidyspid = %s
    """, (tidysp_id,))

    stats = cursor.fetchone()
    review_count = stats['review_count']
    avg_rating = stats['avg_rating']

    # Calculate new average
    if review_count == 0:
        new_avg = new_rating
    else:
        new_avg = ((avg_rating * review_count) + new_rating) / (review_count + 1)

    # Round to 2 decimal places
    new_avg = round(new_avg, 2)

    # Update TidySP metrics
    cursor.execute("""
        SELECT tidyspmetricid 
        FROM tidyspmetrics 
        WHERE tidyspid = %s AND metricname = 'AvgRating'
    """, (tidysp_id,))

    metric = cursor.fetchone()

    if metric:
        # Update existing metric
        cursor.execute("""
            UPDATE tidyspmetric_values 
            SET value = %s, updatedat = NOW() 
            WHERE tidyspmetricid = %s
        """, (new_avg, metric['tidyspmetricid']))
    else:
        # Create new metric
        cursor.execute("""
            INSERT INTO tidyspmetrics (tidyspid, metricname, tableid, createdat) 
            VALUES (%s, 'AvgRating', %s, NOW())
            RETURNING tidyspmetricid
        """, (tidysp_id, tidysp_id))

        metric_id = cursor.fetchone()['tidyspmetricid']

        cursor.execute("""
            INSERT INTO tidyspmetric_values (tidyspmetricid, value, createdat) 
            VALUES (%s, %s, NOW())
        """, (metric_id, new_avg))

    return {
        'tidysp_id': tidysp_id,
        'review_count': review_count + 1,
        'previous_avg_rating': avg_rating,
        'new_avg_rating': new_avg
    }


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Process SNS records
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            # Extract data from message
            review_id = message.get('review_id')
            order_id = message.get('order_id')
            reviewer_id = message.get('reviewer_id')
            reviewer_type = message.get('reviewer_type', 'user')
            reviewee_id = message.get('reviewee_id')
            rating = message.get('rating')
            review_text = message.get('review_text', '')
            is_customer_review = message.get('is_customer_review', True)

            # Connect to database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Get order details
            cursor.execute("""
                SELECT o.orderid, o.userid, o.tidyspid, o.totalprice, sp.productname
                FROM orders o
                LEFT JOIN requests req ON o.requestid = req.requestid
                LEFT JOIN serviceproducts sp ON req.productid = sp.productid
                WHERE o.orderid = %s
            """, (order_id,))

            order_details = cursor.fetchone()

            if not order_details:
                logger.warning(f"Order details not found for order {order_id}")
                continue

            # Get reviewer information
            cursor.execute("""
                SELECT u.email, ud.phonenumber, ud.firstname, ud.lastname
                FROM users u
                JOIN userdetails ud ON u.userid = ud.userid
                WHERE u.userid = %s
            """, (reviewer_id,))

            reviewer_info = cursor.fetchone()

            if not reviewer_info:
                logger.warning(f"Reviewer information not found for user {reviewer_id}")
                continue

            reviewer_name = f"{reviewer_info['firstname']} {reviewer_info['lastname']}"

            # Get reviewee information
            cursor.execute("""
                SELECT u.userid, u.email, ud.phonenumber, ud.firstname, ud.lastname
                FROM users u
                JOIN userdetails ud ON u.userid = ud.userid
                WHERE u.userid = %s
            """, (reviewee_id,))

            reviewee_info = cursor.fetchone()

            if not reviewee_info:
                logger.warning(f"Reviewee information not found for user {reviewee_id}")
                continue

            reviewee_name = f"{reviewee_info['firstname']} {reviewee_info['lastname']}"
            reviewee_email = reviewee_info['email']
            reviewee_phone = reviewee_info['phonenumber']

            # If it's a review for a TidySP, update the TidySP's average rating
            if is_customer_review:
                tidysp_id = order_details['tidyspid']

                # Update the review with the TidySP ID
                cursor.execute("""
                    UPDATE reviews
                    SET tidyspid = %s
                    WHERE reviewid = %s
                """, (tidysp_id, review_id))

                # Update TidySP rating summary
                rating_summary = update_rating_summary(cursor, tidysp_id, rating)
                logger.info(f"Updated rating summary for TidySP {tidysp_id}: {rating_summary}")

            # Create notification for reviewee
            cursor.execute("""
                INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                reviewee_id,
                'REVIEW',
                f"You received a {rating}-star review for order #{order_id}",
                False
            ))

            # Send email notification to reviewee
            if reviewee_email:
                service_name = order_details.get('productname', 'service')

                email_subject = f"New {rating}-Star Review for Your {service_name}"

                # Format email based on who is reviewing whom
                if is_customer_review:
                    # Customer reviewing TidySP
                    email_message = f"""
                    <h2>New Review Received</h2>
                    <p>Dear {reviewee_name},</p>
                    <p>You've received a new review for your {service_name} service (Order #{order_id}).</p>
                    <div style="margin: 20px; padding: 15px; border-left: 4px solid #ccc; background-color: #f9f9f9;">
                        <h3>Rating: {rating} / 5 Stars</h3>
                        <p>"{review_text}"</p>
                        <p style="font-style: italic;">- {reviewer_name}</p>
                    </div>
                    <p>Thank you for your excellent service!</p>
                    <p>TidyZon Team</p>
                    """
                else:
                    # TidySP reviewing Customer
                    email_message = f"""
                    <h2>New Review Received</h2>
                    <p>Dear {reviewee_name},</p>
                    <p>You've received a review from your service provider for Order #{order_id} ({service_name}).</p>
                    <div style="margin: 20px; padding: 15px; border-left: 4px solid #ccc; background-color: #f9f9f9;">
                        <h3>Rating: {rating} / 5 Stars</h3>
                        <p>"{review_text}"</p>
                        <p style="font-style: italic;">- {reviewer_name} (Service Provider)</p>
                    </div>
                    <p>Thank you for using TidyZon!</p>
                    <p>TidyZon Team</p>
                    """

                try:
                    send_email_via_ses(reviewee_email, email_subject, email_message)
                    logger.info(f"Review notification email sent to user {reviewee_id}")
                except Exception as e:
                    logger.error(f"Failed to send email: {e}")

            # Send SMS notification to reviewee if high rating (4-5 stars)
            if reviewee_phone and rating >= 4:
                sms_message = f"You received a {rating}-star review for order #{order_id}! Check your email or app for details."
                try:
                    send_sms_via_twilio(reviewee_phone, sms_message)
                    logger.info(f"Review notification SMS sent to user {reviewee_id}")
                except Exception as e:
                    logger.error(f"Failed to send SMS: {e}")

            connection.commit()

            # Log success to SNS
            log_to_sns(1, 11, 10, 1, {
                "reviewer_id": reviewer_id,
                "reviewee_id": reviewee_id,
                "order_id": order_id,
                "review_id": review_id,
                "rating": rating
            }, "Process Review - Success", reviewer_id)

            logger.info(f"Successfully processed review {review_id} for order {order_id}")

        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Review processed successfully',
                'review_id': review_id if 'review_id' in locals() else None
            })
        }

    except Exception as e:
        logger.error(f"Failed to process review: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 11, 10, 43, {
            "review_id": review_id if 'review_id' in locals() else 'unknown',
            "error": str(e)
        }, "Process Review - Failed", reviewer_id if 'reviewer_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process review',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()