import json
import boto3
import logging
import requests
from datetime import datetime
from psycopg2.extras import RealDictCursor

from serviceRequest.layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# CloudWatch configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Google API Key for Geocoding
GOOGLE_MAPS_API_KEY = secrets["GOOGLE_MAPS_API_KEY"]

# SNS Topic for location updates
LOCATION_UPDATE_TOPIC = secrets["LOCATION_UPDATE_TOPIC"]


def get_current_location():
    try:
        response = requests.get('https://ipinfo.io')
        data = response.json()
        loc = data['loc'].split(',')
        lat, long = float(loc[0]), float(loc[1])
        return {
            'latitude': lat,
            'longitude': long,
            'timestamp': datetime.utcnow().isoformat(),
            'source': 'ip'
        }

    except Exception as e:
        logger.error(f'Couldnt get current location: {e}')


def calculate_google_maps_eta(origin, destination):
    """Calculate estimated time of arrival using Google Maps Directions API"""
    try:
        # Prepare Google Maps Directions API request
        base_url = "https://maps.googleapis.com/maps/api/directions/json"

        params = {
            'origin': f"{origin[0]},{origin[1]}",
            'destination': f"{destination[0]},{destination[1]}",
            'mode': 'driving',  # Assuming service provider is driving
            'departure_time': 'now',  # Use current time for real-time traffic
            'traffic_model': 'best_guess',
            'key': GOOGLE_MAPS_API_KEY
        }

        # Make request to Google Maps API
        response = requests.get(base_url, params=params)
        data = response.json()

        # Check if the request was successful
        if data['status'] != 'OK':
            return {
                'error': f"Google Maps API error: {data['status']}",
                'estimatedArrivalTime': None
            }

        # Extract route information
        route = data['routes'][0]
        leg = route['legs'][0]

        # Calculate arrival time
        duration_in_seconds = leg['duration_in_traffic']['value'] if 'duration_in_traffic' in leg else leg['duration'][
            'value']
        arrival_time = datetime.utcnow().timestamp() + duration_in_seconds

        return {
            'estimatedArrivalTime': datetime.fromtimestamp(arrival_time).isoformat(),
            'duration': leg['duration_in_traffic']['text'] if 'duration_in_traffic' in leg else leg['duration']['text'],
            'distance': leg['distance']['text'],
            'startAddress': leg['start_address'],
            'endAddress': leg['end_address'],
            'polyline': route['overview_polyline']['points']  # For displaying the route on a map
        }

    except Exception as e:
        return {
            'error': str(e),
            'estimatedArrivalTime': None
        }


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Retrieve service details
        body = json.loads(event.get('body', '{}'))
        httpMethod = body.get('httpMethod', '')
        query_params = body.get('queryStringParameters', {}) or {}
        path_params = body.get('pathParameters', {}) or {}

        # Get user and sp ids
        user_id = query_params.get('userid')
        sp_id = query_params.get('tidyspid')

        # Get user location
        cursor.execute("""SELECT address, longitude as lng, latittude as lat FROM requests WHERE userid = %s""", (user_id,))
        user_location = cursor.fetchone()

        # Get service privider details
        cursor.execute(
            """SELECT t.tidyspid, ud.userid, ud.firstname, ud.lastname
            FROM tidysp t JOIN userdetails ud ON t.userid = ud.userid 
            WHERE t.tidyspid = %s""",
            (sp_id,)
        )
        sp_info = cursor.fetchone()

        # Get service provider location
        if httpMethod == 'GET':
            current_location = get_current_location()

            # Calculate ETA
            origin = (current_location['latitude'], current_location['longitude'])
            destination = (user_location['lat'], user_location['lng'])
            eta_info = calculate_google_maps_eta(origin, destination)
        elif httpMethod == 'POST':
            current_location = get_current_location()

            eta_info = {}
            if user_id:
                user_location = user_location
                if user_location and user_location['lat'] and user_location['lng']:
                    origin = (current_location['latitude'], current_location['longitude'])
                    destination = (user_location['lat'], user_location['lng'])
                    eta_info = calculate_google_maps_eta(origin, destination)
        else:
            return {
                'statusCode': 405,
                'body': json.dumps({'error': 'Invalid http method'})
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }








