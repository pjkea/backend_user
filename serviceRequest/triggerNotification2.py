import json
import psycopg2
import logging
import boto3
from psycopg2.extras import RealDictCursor

from serviceRequest.layers.utils import get_secrets, get_db_connection, send_sms_via_twilio, send_email_via_ses, log_to_sns
from twilio.rest import Client

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')
ses_client = boto3.client("ses", region_name="us-east-1")

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def prepare_notification_message(tidysp_info):
    """Prepare a standardized notification message from a record"""
    try:
        # Construct a standardized notification message
        subject = "Service Provider Assigned"

        # Properly formatted message with line breaks
        message = (
            "Your service request has been accepted by a service provider.\n\n"
            f"TidySpID: {tidysp_info.get('tidyspid', 'N/A')}\n"
            f"Name: {tidysp_info.get('firstname', '')} {tidysp_info.get('lastname', '')}\n"
        )

        return subject, message
    except Exception as e:
        logger.error(f"Failed to prepare notification message: {e}")
        raise


def lambda_handler(event, context):
    connection = None
    cursor = None

    # Track all processed records
    results = []

    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        for record in event['Records']:
            record_result = {
                'success': False,
                'record_id': record.get('messageId', 'unknown')
            }

            try:
                message = json.loads(record['Sns']['Message'])

                # Extract data with proper validation
                userid = message.get('userid')
                tidyspid = message.get('tidyspid')
                tidysp_info = message.get('tidyspinfo', {})

                if not all([userid, tidyspid, tidysp_info]):
                    raise ValueError("Missing required fields in message")

                # Get user contact
                cursor.execute("""
                    SELECT ud.phonenumber, u.email 
                    FROM users u 
                    JOIN userdetails ud ON u.userid = ud.userid 
                    WHERE u.userid = %s
                """, (userid,))

                user_info = cursor.fetchone()

                if user_info is None:
                    raise ValueError(f"User not found: {userid}")

                user_number = user_info.get('phonenumber')
                user_email = user_info.get('email')

                # Verify at least one contact method exists
                if not user_number and not user_email:
                    raise ValueError(f"No contact information found for user: {userid}")

                # Prepare notification message
                subject, message_text = prepare_notification_message(tidysp_info)

                # Track what was sent
                notification_sent = []

                # Send email if available
                if user_email:
                    send_email_via_ses(user_email, subject, message_text)
                    notification_sent.append("email")

                # Send SMS if available
                if user_number:
                    send_sms_via_twilio(user_number, message_text)
                    notification_sent.append("sms")


                record_result['success'] = True
                record_result['notification_sent'] = notification_sent
                record_result['userid'] = userid
                record_result['tidyspid'] = tidyspid

                log_to_sns(1, 36, 12, 5, record_result, 'Success', userid)

                logger.info("Successfully sent notification message.")

            except Exception as record_error:
                logger.error(f"Failed to process record: {record_error}")

                record_result['error'] = str(record_error)

                log_to_sns(4, 36, 12, 43, record_result, '', userid)

            # Add this record's result to the overall results
            results.append(record_result)

        # Return summary of all processed records
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': any(r['success'] for r in results),
                'processed_count': len(results),
                'success_count': sum(1 for r in results if r['success']),
                'results': results
            })
        }

    except Exception as e:
        logger.error(f"Lambda execution error: {e}")

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Internal Server Error",
                "error": str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()