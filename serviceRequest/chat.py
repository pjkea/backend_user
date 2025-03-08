import json
import boto3
import logging
import psycopg2
import time
from datetime import datetime

from layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
api_gateway = boto3.client("apigatewaymanagementapi", region_name="us-east-1")

# Connection pooling dictionary - tracks active WebSocket connections
active_connections = {}

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load secrets
secrets = get_secrets()


def handle_connect(event, connection_id):
    """Handle new WebSocket connections and store user info"""
    try:
        # Get user identification from query parameters
        user_id = event.get('queryStringParameters', {}).get('userId')
        user_type = event.get('queryStringParameters', {}).get('role')  # 'user' or 'provider'

        if not user_id or not user_type:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing userId or userType parameter'})
            }

        # Store connection data in memory (for this Lambda instance)
        active_connections[connection_id] = {
            'userId': int(user_id),
            'userType': user_type,
            'connectedAt': int(time.time()),
            'status': 'ONLINE'
        }

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Connected successfully'})
        }
    except Exception as e:
        print(f"Error in handle_connect: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal server error'})
        }


def handle_disconnect(connection_id):
    """Handle WebSocket disconnections"""
    try:
        # Remove connection from in-memory tracking
        if connection_id in active_connections:
            del active_connections[connection_id]

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Disconnected successfully'})
        }
    except Exception as e:
        print(f"Error in handle_disconnect: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal server error'})
        }


def handle_message(event, connection_id, api_gateway_client):
    """Process incoming messages and relay to recipients"""
    try:
        # Get sender information from connection tracking
        if connection_id not in active_connections:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Connection not registered'})
            }

        sender_info = active_connections[connection_id]
        sender_id = sender_info['userId']

        # Parse message body
        body = json.loads(event.get('body', '{}'))
        message_text = body.get('message')
        receiver_id = body.get('receiverId')
        order_id = body.get('orderId')
        thread_id = body.get('threadId', 0)  # Default to 0 if not provided

        if not message_text or not receiver_id or not order_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required message parameters'})
            }

        # Create message record
        time_requested = datetime.now()

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""INSERT INTO inappmessages (orderid, senderid, receiverid, threadid, messagetext, timerequested, createdat)
            VALUES (%s, %s, %s, %s, %s, %s, NOW()) RETURNING messageid""",
            (order_id, sender_id, receiver_id, thread_id, message_text, time_requested))

        message_id = cursor.fetchone()[0]

        # Find recipient's active connections
        recipient_connections = []
        for conn_id, conn_info in active_connections.items():
            if conn_info['userId'] == int(receiver_id):
                recipient_connections.append(conn_id)

        # Message sent timestamp
        time_sent = datetime.now()

        cursor.execute("""UPDATE inappmessages SET timesent = %s WHERE messageid = %s""", (time_sent, message_id))

        # Construct message payload for WebSocket
        message_payload = json.dumps({
            'messageId': message_id,
            'orderId': order_id,
            'threadId': thread_id,
            'senderId': sender_id,
            'message': message_text,
            'timeRequested': time_requested.isoformat(),
            'timeSent': time_sent.isoformat()
        })

        # Send message to all recipient's active connections
        delivery_success = False
        time_received = None

        for recipient_conn_id in recipient_connections:
            try:
                api_gateway_client.post_to_connection(
                    ConnectionId=recipient_conn_id,
                    Data=message_payload
                )
                delivery_success = True
                time_received = datetime.now()
            except Exception as e:
                print(f"Failed to deliver to connection {recipient_conn_id}: {str(e)}")

        # Update received timestamp if delivered
        if delivery_success and time_received:
            cursor.execute("""UPDATE inappmessages SET timereceived = %s WHERE messageid = %s""",
                (time_received, message_id))

        # Send delivery confirmation to sender
        delivery_status = {
            'messageId': message_id,
            'delivered': delivery_success,
            'timeSent': time_sent.isoformat(),
            'timeReceived': time_received.isoformat() if time_received else None
        }

        api_gateway_client.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(delivery_status)
        )

        # Close database connection
        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Message sent', 'messageId': message_id})
        }
    except Exception as e:
        print(f"Error in handle_message: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal server error'})
        }


def handle_get_history(event, connection_id, api_gateway_client):
    """Retrieve chat history for a conversation"""
    try:
        # Get requester information
        if connection_id not in active_connections:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Connection not registered'})
            }

        requester_info = active_connections[connection_id]
        requester_id = requester_info['userId']

        # Parse request body
        body = json.loads(event.get('body', '{}'))
        order_id = body.get('orderId')
        other_user_id = body.get('otherUserId')
        thread_id = body.get('threadId')

        if not order_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing orderId parameter'})
            }

        # Connect to PostgreSQL
        conn = get_db_connection()
        cursor = conn.cursor()

        # Build query based on provided parameters
        query = """SELECT messageid, orderid, senderid, receiverid, threadid, messagetext, timerequested, timesent, timereceived
            FROM inappmessages WHERE orderid = %s"""
        query_params = [order_id]

        # Add thread filter if specified
        if thread_id is not None:
            query += " AND threadid = %s"
            query_params.append(thread_id)

        # Add user filters if specified
        if other_user_id is not None:
            query += " AND ((senderid = %s AND receiverid = %s) OR (senderis = %s AND receiverid = %s))"
            query_params.extend([requester_id, other_user_id, other_user_id, requester_id])
        else:
            query += " AND (senderid = %s OR receiverid = %s)"
            query_params.extend([requester_id, requester_id])

        # Order by time requested
        query += " ORDER BY timerequested"

        # Execute query
        cursor.execute(query, query_params)
        rows = cursor.fetchall()

        # Format results
        messages = []
        for row in rows:
            messages.append({
                'messageId': row[0],
                'orderId': row[1],
                'senderId': row[2],
                'receiverId': row[3],
                'threadId': row[4],
                'messageText': row[5],
                'timeRequested': row[6].isoformat() if row[6] else None,
                'timeSent': row[7].isoformat() if row[7] else None,
                'timeReceived': row[8].isoformat() if row[8] else None
            })

        # Close database connection
        cursor.close()
        conn.close()

        # Send message history to requester
        api_gateway_client.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps({
                'type': 'messageHistory',
                'orderId': order_id,
                'threadId': thread_id,
                'messages': messages
            })
        )

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'History retrieved successfully'})
        }
    except Exception as e:
        print(f"Error in handle_get_history: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Internal server error'})
        }


def lambda_handler(event, context):
    """Main handler for real-time chat WebSocket events"""
    route_key = event.get('requestContext', {}).get('routeKey')
    connection_id = event.get('requestContext', {}).get('connectionId')
    domain_name = event.get('requestContext', {}).get('domainName')
    stage = event.get('requestContext', {}).get('stage')

    # Set up API Gateway management client for the specific endpoint
    if domain_name and stage:
        api_gateway_endpoint = f"https://{domain_name}/{stage}"
        api_gateway_client = boto3.client('apigatewaymanagementapi', endpoint_url=api_gateway_endpoint)

    # Route to appropriate handler based on WebSocket route
    if route_key == '$connect':
        return handle_connect(event, connection_id)
    elif route_key == '$disconnect':
        return handle_disconnect(connection_id)
    elif route_key == 'sendMessage':
        return handle_message(event, connection_id, api_gateway_client)
    elif route_key == 'getHistory':
        return handle_get_history(event, connection_id, api_gateway_client)
    else:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Unsupported route'})
        }




