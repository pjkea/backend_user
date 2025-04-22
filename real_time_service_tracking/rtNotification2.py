import json
import boto3
import logging

from datetime import datetime
from psycopg2.extras import RealDictCursor
from layers.utils import get_secrets, get_db_connection, log_to_sns

# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')
apigateway = boto3.client('apigatewaymanagementapi')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topic
SNS_LOGGING_TOPIC_ARN = secrets['SNS_LOGGING_TOPIC_ARN']

# WebSocket API endpoint
WEBSOCKET_API_ENDPOINT = secrets['WEBSOCKET_API_ENDPOINT']


def update_message(conn, messageid):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    timesent = datetime.now()

    cursor.execute("UPDATE inappmessages SET timesent = %s WHERE messageid = %s",
                   (timesent, messageid))
    conn.commit()
    cursor.close()


def get_connection_id(conn, userid):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT connectionid FROM connections WHERE userid = %s AND active = TRUE ORDER BY connectedat DESC LIMIT 1",
                   (userid,))
    result = cursor.fetchone()
    cursor.close()
    return result


def send_websocket_message(connectionid, message_data):
    """Send a message to a client via WebSocket"""
    try:
        # Format API endpoint URL for the ApiGatewayManagementApi client
        endpoint_url = f"https://{WEBSOCKET_API_ENDPOINT.split('/')[-1]}"

        # Create a client for the specific connection
        client = boto3.client('apigatewaymanagementapi',
                              endpoint_url=endpoint_url)

        # Send the message
        response = client.post_to_connection(
            ConnectionId=connectionid,
            Data=json.dumps(message_data).encode('utf-8')
        )
        return True
    except client.exceptions.GoneException:
        # Connection is no longer available
        print(f"Connection {connectionid} is gone. User likely disconnected.")
        # Mark connection as inactive in your database
        mark_connection_inactive(connectionid)
        return False
    except Exception as e:
        print(f"Error sending WebSocket message: {str(e)}")
        return False

def mark_connection_inactive(conn, connectionid):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("UPDATE connections SET active = FALSE WHERE connectionid = %s",(connectionid,))
    conn.commit()
    cursor.close()


def lambda_handler(event, context):
    try:
        for record in event['Records']:
            connection = get_db_connection()
            sns_message = json.loads(record['Sns']['Message'])
            payload = sns_message['payload']

            messageid = payload.get('message')
            userid = payload.get('userid')
            message_type = payload.get('type')
            message = payload.get('notification')

            connectionid = get_connection_id(connection, userid)
            if not connectionid:
                print(f"No active WebSocket connection found for user {userid}")
                continue

            websocket_payload = {
                'type': message_type,
                'messageid': messageid,
                'trackingid': payload.get('trackingid'),
                'message': message,
                'userid': userid,
                'orderid': payload.get('orderid'),
                'timestamp': datetime.now().isoformat(),
                'status': payload.get('status')
            }

            if message_type == 'milestone':
                websocket_payload['milestone'] = payload.get('milestone')

            success = send_websocket_message(connectionid, websocket_payload)

            if success and messageid:
                update_message(connection, messageid)

            log_to_sns(1,2,3,4, websocket_payload, 'Push Notifications', userid)

            logger.info("Notifications sent successfully")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Processed notification events successfully {websocket_payload}')
        }

    except Exception as e:
        logger.error(f'Unexpected error: {str(e)}')

        log_to_sns(1,2,3,4, {str(e)}, '', userid)

        return {
            'statusCode': 500,
            'body': json.dumps(f'Unexpected error: {str(e)}')
        }















