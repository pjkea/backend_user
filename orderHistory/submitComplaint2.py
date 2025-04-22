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


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Process SNS records
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            # Extract data from message
            complaint_id = message.get('complaint_id')
            ticket_id = message.get('ticket_id')
            order_id = message.get('order_id')
            user_id = message.get('user_id')
            is_customer = message.get('is_customer')
            complaint_type = message.get('complaint_type')
            complaint_text = message.get('complaint_text')
            severity = message.get('severity')
            order_details = message.get('order_details', {})

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

            # Assign ticket to a customer support agent (simplified here)
            cursor.execute("""
                SELECT userid FROM supportagents
                WHERE is_active = TRUE
                ORDER BY assigned_tickets_count ASC
                LIMIT 1
            """)

            agent_result = cursor.fetchone()

            agent_id = None
            if agent_result:
                agent_id = agent_result['userid']

                # Update the ticket with the assigned agent
                cursor.execute("""
                    UPDATE supporttickets
                    SET assigned_to = %s, status = 'ASSIGNED', updatedat = NOW()
                    WHERE ticketid = %s
                """, (agent_id, ticket_id))

                # Update agent's assigned ticket count
                cursor.execute("""
                    UPDATE supportagents
                    SET assigned_tickets_count = assigned_tickets_count + 1
                    WHERE userid = %s
                """, (agent_id,))

            # Update complaint status
            cursor.execute("""
                UPDATE complaints
                SET status = 'PROCESSING', updatedat = NOW()
                WHERE complaintid = %s
            """, (complaint_id,))

            # Add a note to the complaint
            note = "Complaint received and is being processed by our support team."
            cursor.execute("""
                INSERT INTO complaintnotes (complaintid, note, createdat)
                VALUES (%s, %s, NOW())
            """, (complaint_id, note))

            # Get other party information (customer or TidySP)
            if is_customer:
                # Get TidySP info
                cursor.execute("""
                    SELECT t.tidyspid, t.userid as tidysp_userid, ud.firstname, ud.lastname
                    FROM orders o
                    JOIN tidysp t ON o.tidyspid = t.tidyspid
                    JOIN userdetails ud ON t.userid = ud.userid
                    WHERE o.orderid = %s
                """, (order_id,))

                other_party = cursor.fetchone()
                other_party_type = "Service Provider"
            else:
                # Get Customer info
                cursor.execute("""
                    SELECT o.userid as customer_userid, ud.firstname, ud.lastname
                    FROM orders o
                    JOIN userdetails ud ON o.userid = ud.userid
                    WHERE o.orderid = %s
                """, (order_id,))

                other_party = cursor.fetchone()
                other_party_type = "Customer"

            other_party_name = f"{other_party['firstname']} {other_party['lastname']}" if other_party else "Unknown"

            # Save all changes
            connection.commit()

            # Send acknowledgment to user
            email_subject = f"Your Complaint #{complaint_id} has been received"
            email_message = f"""
            <h2>Complaint Received</h2>
            <p>Dear {user_name},</p>
            <p>We have received your complaint regarding order #{order_id}. Our support team is reviewing the details and will address your concerns promptly.</p>
            <p><strong>Complaint Details:</strong><br>
            Type: {complaint_type}<br>
            Severity: {severity}<br>
            Reference #: {complaint_id}<br>
            Support Ticket #: {ticket_id}</p>
            <p>You will receive updates as we investigate this matter. If you have any additional information or questions, please reply to this email or contact our support team.</p>
            <p>Thank you for bringing this to our attention.</p>
            <p>Sincerely,<br>
            The TidyZon Support Team</p>
            """

            sms_message = f"Your complaint #{complaint_id} for order #{order_id} has been received. Our support team will contact you shortly. Ticket #: {ticket_id}"

            # Send email and SMS
            if user_email:
                try:
                    send_email_via_ses(user_email, email_subject, email_message)
                    logger.info(f"Acknowledgment email sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send email: {e}")

            if user_phone:
                try:
                    send_sms_via_twilio(user_phone, sms_message)
                    logger.info(f"Acknowledgment SMS sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send SMS: {e}")

            # Create notification in app
            cursor.execute("""
                INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
            user_id, 'COMPLAINT', f"Your complaint #{complaint_id} has been received and is being processed.", False))

            # Log success to SNS
            log_to_sns(1, 11, 20, 1, {
                "user_id": user_id,
                "order_id": order_id,
                "complaint_id": complaint_id,
                "ticket_id": ticket_id,
                "status": "PROCESSING"
            }, "Process Complaint - Success", user_id)

            logger.info(f"Successfully processed complaint {complaint_id} for order {order_id}")

            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Complaint processed successfully',
                    'complaint_id': complaint_id,
                    'ticket_id': ticket_id,
                    'status': 'PROCESSING'
                })
            }

    except Exception as e:
        logger.error(f"Failed to process complaint: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 11, 20, 43, {
            "complaint_id": complaint_id if 'complaint_id' in locals() else 'unknown',
            "error": str(e)
        }, "Process Complaint - Failed", user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process complaint',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()