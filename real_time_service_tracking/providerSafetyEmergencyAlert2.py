import json
import boto3
import logging

from datetime import datetime
from psycopg2.extras import RealDictCursor
from layers.utils import get_secrets, get_db_connection, log_to_sns


# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topic
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']


def send_push_notification(user_id, title, message, data=None):
    """Send push notification to a user"""
    try:
        # In a real implementation, this would use a service like Amazon SNS Mobile Push
        # or Firebase Cloud Messaging (FCM) to send push notifications
        logger.info(f"Sending push notification to user {user_id}: {title} - {message}")

        # Placeholder for actual push notification logic
        return True
    except Exception as e:
        logger.error(f"Error sending push notification: {str(e)}")
        return False


def send_sms(phone_number, message):
    """Send SMS using AWS SNS"""
    try:
        if not phone_number:
            logger.warning("No phone number provided for SMS")
            return False

        # Send SMS using Amazon SNS
        response = sns_client.publish(
            PhoneNumber=phone_number,
            Message=message,
            MessageAttributes={
                'AWS.SNS.SMS.SenderID': {
                    'DataType': 'String',
                    'StringValue': 'TIDYAPP'
                },
                'AWS.SNS.SMS.SMSType': {
                    'DataType': 'String',
                    'StringValue': 'Transactional'
                }
            }
        )
        logger.info(f"SMS sent: {response['MessageId']}")
        return True
    except Exception as e:
        logger.error(f"Error sending SMS: {str(e)}")
        return False


def lambda_handler(event, context):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor
                          )
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            alertid = message.get('alertid')
            tidyspid = message.get('tidyspid')
            userid = message.get('userid')
            orderid = message.get('orderid')
            alert_type = message.get('alert_type')
            description = message.get('description')
            location = message.get('location')
            alert_time = message.get('time')

            if not all([alertid, tidyspid, orderid, alert_type, description, location]):
                logger.error("Missing required parameters in SNS message")
                return

            try:
                cursor.execute("""SELECT ud.firstname, ud.phonenumber FROM users u 
                JOIN userdetails ud ON u.userid = ud.userid WHERE u.userid = %s""", (userid,))

                sp_details = cursor.fetchone()
                sp_name = sp_details.get('firstname')
                sp_number = sp_details.get('phonenumber')

                cursor.execute("""SELECT userid FROM orders WHERE orderid = %s""", (orderid,))
                customerid = cursor.fetchone()[0]
                cursor.execute("""SELECT u.username, ud.phonenumber FROM users u 
                JOIN userdetails ud ON u.userid = ud.userid WHERE u.userid = %s""", (customerid,))

                customer_details = cursor.fetchone()
                customer_name = customer_details.get('firstname')
                customer_number = customer_details.get('phonenumber')

                cursor.execute("""SELECT u.userid, ud.firstname, ud.phonenumber FROM users u
                JOIN userdetails ud ON u.userid = ud.userid WHERE u.roleid = 3 AND u.isactive = 1""")

                admins = cursor.fetchone()
                adminid = admins.get('userid')
                admin_name = admins.get('firstname')
                admin_phone = admins.get('phonenumber')

                if customerid:
                    note = f'EMERGENCY ALERT: Your service provider {sp_name} has triggered an emergency alert.'
                    cursor.execute("""INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                    VALUES (%s, %s, %s, %s, %s)""", (userid, 'provider emergency', note, 0, datetime.utcnow()))

                    send_push_notification(userid,
                                           "Emergency Alert",
                                           f"Your service provider {sp_name} has triggered an emergency alert.",
                                           {
                                               'alert_id': alertid,
                                               'order_id': orderid,
                                               'alert_type': alert_type,
                                               'notification_type': 'provider_emergency'
                                           }
                                           )
                    if customer_number:
                        sms_message = f"TIDYAPP ALERT: Your service provider {sp_name} has triggered an emergency alert. Please check the app for details."
                        send_sms(customer_number, sms_message)

                location_str = ""
                if location.get('latitude') and location.get('longitude'):
                    location_str = f" at location ({location.get('latitude')}, {location.get('longitude')})"

                admin_message = f"EMERGENCY ALERT: Provider {sp_name} (ID: {tidyspid}) has triggered an {alert_type} alert{location_str}. Description: {description}"

                for adminid, admin_phone in admins:
                    cursor.execute("""INSERT INTO notifications (user_id, notification_type, message, is_read, created_at)
                    VALUES (%s, %s, %s, %s, %s)""", (adminid, 'admin emergency alert', admin_message, 0, datetime.utcnow()))

                    if admin_phone:
                        sms_message = f"TIDYAPP EMERGENCY: Provider {sp_name} (ID: {tidyspid}) needs immediate assistance. Check admin panel."
                        send_sms(admin_phone, sms_message)

                conn.commit()

                log_to_sns(1,2,3,4,'', '', userid)

                logger.info(f"Successfully processed emergency alert {alertid} for provider {tidyspid}")


            except Exception as e:
                conn.rollback()
                logger.error(f"Database error: {str(e)}")
            finally:
                cursor.close()
                conn.close()

    except Exception as e:
        logger.error(f"Error processing emergency alert: {str(e)}")
        raise e



