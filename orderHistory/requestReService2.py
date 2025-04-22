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
ASSIGN_PROVIDER_TOPIC_ARN = secrets["ASSIGN_PROVIDER_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Process SNS records
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            # Extract data from message
            request_id = message.get('request_id')
            user_id = message.get('user_id')
            original_order_id = message.get('original_order_id')
            original_tidysp_id = message.get('original_tidysp_id')
            service_details = message.get('service_details', {})

            # Connect to database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Get user contact information
            cursor.execute("""
                SELECT u.email, ud.phonenumber, ud.firstname, ud.lastname
                FROM users u
                JOIN userdetails ud ON u.userid = ud.userid
                WHERE u.userid = %s
            """, (user_id,))

            user_info = cursor.fetchone()

            if not user_info:
                logger.warning(f"User information not found for user {user_id}")
                continue

            user_email = user_info['email']
            user_phone = user_info['phonenumber']
            user_name = f"{user_info['firstname']} {user_info['lastname']}"

            # Check if the original TidySP is available
            tidysp_id = None
            if original_tidysp_id:
                cursor.execute("""
                    SELECT t.tidyspid, u.email, ud.phonenumber, ud.firstname, ud.lastname
                    FROM tidysp t
                    JOIN users u ON t.userid = u.userid
                    JOIN userdetails ud ON t.userid = ud.userid
                    WHERE t.tidyspid = %s AND t.isactive = TRUE
                """, (original_tidysp_id,))

                tidysp_info = cursor.fetchone()

                if tidysp_info:
                    tidysp_id = tidysp_info['tidyspid']

                    # Send notification to the original TidySP
                    tidysp_email = tidysp_info['email']
                    tidysp_phone = tidysp_info['phonenumber']
                    tidysp_name = f"{tidysp_info['firstname']} {tidysp_info['lastname']}"

                    # Create notification in app for TidySP
                    cursor.execute("""
                        INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                        VALUES (%s, %s, %s, %s, NOW())
                    """, (
                        tidysp_id,
                        'RE_SERVICE',
                        f"A customer has requested a re-service. Request ID: {request_id}",
                        False
                    ))

                    # Send email to TidySP
                    if tidysp_email:
                        scheduled_date = service_details.get('scheduled_datetime', 'Flexible')
                        product_name = service_details.get('product_name', 'Service')
                        address = service_details.get('address', 'N/A')

                        email_subject = f"New Re-Service Request for {product_name}"
                        email_message = f"""
                        <h2>New Re-Service Request</h2>
                        <p>Dear {tidysp_name},</p>
                        <p>A customer has requested a re-service for a previous job you completed.</p>
                        <div style="margin: 20px; padding: 15px; border-left: 4px solid #4CAF50; background-color: #f9f9f9;">
                            <p><strong>Request ID:</strong> {request_id}</p>
                            <p><strong>Service:</strong> {product_name}</p>
                            <p><strong>Customer:</strong> {user_name}</p>
                            <p><strong>Scheduled Date:</strong> {scheduled_date}</p>
                            <p><strong>Address:</strong> {address}</p>
                        </div>
                        <p>Please log in to the app to accept or decline this request.</p>
                        <p>Thank you for providing quality service!</p>
                        <p>TidyZon Team</p>
                        """

                        try:
                            send_email_via_ses(tidysp_email, email_subject, email_message)
                            logger.info(f"Re-service notification email sent to TidySP {tidysp_id}")
                        except Exception as e:
                            logger.error(f"Failed to send email to TidySP: {e}")

                    # Send SMS to TidySP
                    if tidysp_phone:
                        sms_message = f"New re-service request from a previous customer. Request ID: {request_id}. Please check your app for details."
                        try:
                            send_sms_via_twilio(tidysp_phone, sms_message)
                            logger.info(f"Re-service notification SMS sent to TidySP {tidysp_id}")
                        except Exception as e:
                            logger.error(f"Failed to send SMS to TidySP: {e}")

            # Create a new order for this request
            cursor.execute("""
                INSERT INTO orders (requestid, userid, tidyspid, status, totalprice, createdat)
                VALUES (%s, %s, %s, %s, %s, NOW())
                RETURNING orderid
            """, (
                request_id,
                user_id,
                tidysp_id,  # May be NULL if original TidySP not available
                'PENDING' if tidysp_id else 'OPEN',
                service_details.get('price', 0)
            ))

            new_order = cursor.fetchone()
            new_order_id = new_order['orderid']

            # Insert order details
            cursor.execute("""
                INSERT INTO orderdetails (orderid, add_ons, notes, createdat)
                VALUES (%s, %s, %s, NOW())
            """, (
                new_order_id,
                json.dumps(service_details.get('add_ons', [])) if isinstance(service_details.get('add_ons'),
                                                                             (list, dict)) else service_details.get(
                    'add_ons'),
                f"Re-service request for order {original_order_id}"
            ))

            # Create notification for user
            cursor.execute("""
                INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'RE_SERVICE',
                f"Your re-service request has been submitted. Order ID: {new_order_id}",
                False
            ))

            # Send email notification to user
            if user_email:
                email_subject = "Re-Service Request Confirmation"
                email_message = f"""
                <h2>Re-Service Request Confirmation</h2>
                <p>Dear {user_name},</p>
                <p>Your request for a re-service has been submitted successfully.</p>
                <div style="margin: 20px; padding: 15px; border-left: 4px solid #4CAF50; background-color: #f9f9f9;">
                    <p><strong>Order ID:</strong> {new_order_id}</p>
                    <p><strong>Service:</strong> {service_details.get('product_name', 'Service')}</p>
                    <p><strong>Scheduled Date:</strong> {service_details.get('scheduled_datetime', 'To be confirmed')}</p>
                </div>
                """

                if tidysp_id:
                    email_message += f"""
                    <p>We have sent a notification to your previous service provider. You will be notified once they accept the request.</p>
                    """
                else:
                    email_message += f"""
                    <p>We will assign a service provider for you shortly.</p>
                    """

                email_message += """
                <p>Thank you for choosing TidyZon!</p>
                <p>TidyZon Team</p>
                """

                try:
                    send_email_via_ses(user_email, email_subject, email_message)
                    logger.info(f"Re-service confirmation email sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send email to user: {e}")

            # If no TidySP assigned yet, send to provider assignment system
            if not tidysp_id:
                # Prepare message for provider assignment
                assignment_message = {
                    'requestid': request_id,
                    'orderid': new_order_id,
                    'userid': user_id,
                    'servicedetails': service_details
                }

                # Publish to provider assignment topic
                sns_client.publish(
                    TopicArn=ASSIGN_PROVIDER_TOPIC_ARN,
                    Message=json.dumps(assignment_message),
                    Subject=f'Assign Provider for Re-Service: {request_id}'
                )

                logger.info(f"Provider assignment request sent for re-service request {request_id}")

            connection.commit()

            # Log success to SNS
            log_to_sns(1, 11, 12, 1, {
                "user_id": user_id,
                "request_id": request_id,
                "new_order_id": new_order_id,
                "tidysp_assigned": tidysp_id is not None
            }, "Process Re-Service - Success", user_id)

            logger.info(f"Successfully processed re-service request {request_id}")

            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Re-service request processed successfully',
                    'request_id': request_id,
                    'order_id': new_order_id,
                    'tidysp_assigned': tidysp_id is not None
                })
            }

    except Exception as e:
        logger.error(f"Failed to process re-service request: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 11, 12, 43, {
            "request_id": request_id if 'request_id' in locals() else 'unknown',
            "error": str(e)
        }, "Process Re-Service - Failed", user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process re-service request',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()