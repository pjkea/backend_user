import json
import boto3
import logging
import requests
from datetime import datetime
from psycopg2.extras import RealDictCursor, Json
from layers.utils import get_secrets, get_db_connection, calculate_eta_google

# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Mapping service
MAPPING_SERVICE = secrets['MAPPING_SERVICE']


def format_duration(seconds):
    """Format duration in seconds to a readable text"""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours > 0:
        return f"{hours} hour(s) {minutes} min"
    else:
        return f"{minutes} min"


def format_distance(meters):
    """Format distance in meters to a readable text"""
    if meters >= 1000:
        kilometers = meters / 1000.0
        return f"{kilometers:.1f} km"
    else:
        return f"{meters} m"


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        body = json.loads(event.get('body', '{}'))

        orderid = body.get('orderid')
        userid = body.get('userid')
        trackingid = body.get('trackingid')
        tidyspid = body.get('tidyspid')
        location_data = body.get('currentlocation', {})

        if not all([orderid, userid, trackingid, location_data]):
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameters',
                    'orderid': orderid,
                    'userid': userid
                })
            }

        curr_lat = location_data.get('latitude')
        curr_lng = location_data.get('longitude')

        if not all([curr_lat, curr_lng]):
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing latitude or longitude in current location',
                    'orderid': orderid,
                    'userid': userid
                })
            }

        # Get database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Get user's destination location
        cursor.execute("""SELECT latitude, longitude FROM userdetails WHERE userid = %s""", (userid,))
        user_location = cursor.fetchone()

        if not user_location:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'message': 'User location not found',
                    'orderid': orderid,
                    'userid': userid
                })
            }

        dest_lat = user_location.get('latitude')
        dest_lng = user_location.get('longitude')

        # Format user destination as JSON
        user_location_json = {
            'latitude': dest_lat,
            'longitude': dest_lng
        }

        # Calculate ETA using Google Maps
        eta_data = calculate_eta_google(curr_lat, curr_lng, dest_lat, dest_lng)

        # Get current timestamp
        timestamp = datetime.now()

        # Update tracking table with new ETA
        cursor.execute("""UPDATE tracking SET arrivaltime = %s, userlocation = %s, updated_at = %s  WHERE trackingid = %s
            """,(Json(eta_data), Json(user_location_json), timestamp, trackingid))

        # Record ETA in tracking_history
        cursor.execute("""INSERT INTO tracking_history (trackingid, status, timestamp, data, userid, orderid, tidyspid)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (trackingid, 'eta_calculated', timestamp,
                Json({'eta': eta_data,'current_location': location_data,'destination_location': user_location_json,'event': 'eta_updated'}),
                           userid,orderid,tidyspid))

        # Check if this is a significant ETA change that needs notification
        cursor.execute("""
            SELECT data->'eta'->>'eta_seconds' AS previous_eta_seconds
            FROM tracking_history
            WHERE trackingid = %s
            AND status = 'eta_calculated'
            AND timestamp < %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,(trackingid, timestamp))

        previous_eta = cursor.fetchone()

        connection.commit()

        logger.info(f"Successfully logged ETA for tracking ID: {trackingid}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'success',
                'orderid': orderid,
                'userid': userid,
                'trackingid': trackingid,
                'servicelocation': user_location_json,
                'location': location_data,
                'eta': eta_data,
                'eta_text': format_duration(int(eta_data.get('eta_seconds', 0)))
            })
        }

    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f'Failed to calculate ETA: {str(e)}')

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Failed to calculate ETA: {str(e)}',
                'orderid': orderid,
                'userid': userid
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()