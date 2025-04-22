import json
import boto3
import logging
import time
from datetime import datetime
import uuid

from psycopg2.extras import RealDictCursor, Json
from layers.utils import get_secrets, get_db_connection, calculate_distance

# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topic
NOTIFICATIONS_TOPIC_ARN = secrets['NOTIFICATIONS_TOPIC_ARN']

# Notification Conditions
proximity_meters = 500
milestones = [5000, 2000, 1000, 500]
delay_minutes = 5


def inappmessages(conn, orderid, senderid, receiverid, threadid, message):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    timestamp = datetime.now()

    cursor.execute("""INSERT INTO inappmessages (orderid, senderid, receiverid, threadid, messagetext, timerequested, timesent)
    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING messageid""", (orderid, senderid, receiverid, threadid, message, timestamp, timestamp))

    messageid = cursor.fetchone()[0]
    conn.commit()

    cursor.close()


    return messageid


def lambda_handler(event, context):
    try:
        body = {}
        if event.get('body'):
            body = json.loads(event.get('body', {}))
        elif event.get('detail'):
            body = json.loads(event.get('detail', {}))

        orderid = body.get('orderid')

        if not orderid:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': f'Missing {orderid} parameter'})
            }

        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""SELECT * FROM tracking WHERE orderid = %s""", (orderid,))
        tracking_session = cursor.fetchone()

        if tracking_session.get('is_paused', False):
            return {
                'statusCode': 200,
                'body': json.dumps({'message': f'This order has been paused: {orderid}'})
            }

        userid = tracking_session.get('userid')
        tidyspid = tracking_session.get('tidyspid')
        trackingid = tracking_session.get('trackingid')[0]
        user_location = tracking_session.get('userlocation')
        sp_location = tracking_session.get('splocation')

        if not user_location or not sp_location:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': f'Missing {user_location} parameter'})
            }

        usr_lat = user_location.get('latitude')
        usr_lng = user_location.get('longitude')
        sp_lat = sp_location.get('latitude')
        sp_lng = sp_location.get('longitude')

        distance = calculate_distance(sp_lat, sp_lng, usr_lat, usr_lng)

        notifications = []

        if distance <= proximity_meters:
            notification = f'Provider is within {int(distance)} meters of your location'
            threadid = int(uuid.uuid4())
            push_message = inappmessages(connection, orderid, tidyspid, userid, threadid, notification)

            payload = {
                'type': 'proximity',
                'message': push_message,
                'orderid': orderid,
                'userid': userid,
                'trackingid': trackingid,
                'tidyspid': tidyspid,
                'notification': notification,
                'timestamp': datetime.now(),
                'status': 'near_location'
            }

            sns_client.publish(
                TopicArn=NOTIFICATIONS_TOPIC_ARN,
                Message=json.dumps(payload),
            )
            logger.info(f'Successfully sent proximity alert.')
            notifications.append(payload)

        for milestone in milestones:
            if distance == milestone:
                notification = f'Provider is within {int(milestone)} meters of your location'
                threadid = int(uuid.uuid4())
                push_message = inappmessages(connection, orderid, tidyspid, userid, threadid, notification)
                payload = {
                    'type': 'milestone',
                    'message': push_message,
                    'trackingid': trackingid,
                    'orderid': orderid,
                    'userid': userid,
                    'tidyspid': tidyspid,
                    'notification': notification,
                    'milestone': milestone,
                    'timestamp': datetime.now(),
                    'status': 'driving'
                }
                sns_client.publish(
                    TopicArn=NOTIFICATIONS_TOPIC_ARN,
                    Message=json.dumps(payload),
                )
                logger.info(f'Successfully sent alert.')
                notifications.append(payload)

        cursor.execute("""SELECT data->>'eta_seconds' AS eta_seconds FROM trackinghistory WHERE trackingid = %s
        AND data ? 'eta_seconds' ORDER BY timestamp DESC LIMIT 1""", (trackingid,))

        previous_eta = cursor.fetchone()
        previous_etaseconds = float(previous_eta['eta_seconds'])
        current_eta = body.get('eta')

        if current_eta and previous_etaseconds:
            current_etaseconds = float(current_eta['eta_seconds'])
            eta_change = (current_etaseconds - previous_etaseconds) / 60

            if eta_change >= delay_minutes:
                notification = f'ETA has increased by {int(eta_change)} minutes'
                threadid = str(uuid.uuid4())

                push_message = inappmessages(connection, orderid, tidyspid, userid, threadid, notification)

                payload = {
                    'type': 'delay',
                    'message': push_message,
                    'trackingid': trackingid,
                    'orderid': orderid,
                    'userid': userid,
                    'tidyspid': tidyspid,
                    'notification': notification,
                    'timestamp': datetime.now().isoformat(),
                    'status': 'delayed'
                }

                sns_client.publish(
                    TopicArn=NOTIFICATIONS_TOPIC_ARN,
                    Message=json.dumps(payload),
                )

                cursor.execute("""INSERT INTO trackinghistory (trackingid, status, timestamp, data, userid, orderid, tidyspid)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (trackingid, 'delay', datetime.now(), Json({'previous_eta_seconds': previous_etaseconds,'current_eta_seconds': current_etaseconds,'eta_change_minutes': eta_change}), userid, orderid, tidyspid))
                connection.commit()

                logger.info(f'Successfully sent delay alert: {trackingid}')
                notifications.append(payload)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Notification check completed, sent {len(notifications)} notifications',
                'notifications': notifications
            })
        }

    except Exception as e:
        logger.error(f'Failed to notify user {orderid}, {e}')

        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Error processing notification: {str(e)}'})
        }

    finally:
        cursor.close()
        connection.close()



