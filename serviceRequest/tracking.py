import json
import boto3
import logging
import requests
from datetime import datetime
from psycopg2.extras import RealDictCursor

from serviceRequest.layers.utils import get_secrets, get_db_connection, log_to_sns, calculate_google_maps_eta

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


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Retrieve service details
        body = json.loads(event.get('body', '{}'))
        httpmethod = body.get('httpmethod', '')
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
        if httpmethod == 'GET':
            current_location = get_current_location()

            # Calculate ETA
            origin = (current_location['latitude'], current_location['longitude'])
            destination = (user_location['lat'], user_location['lng'])
            eta_info = calculate_google_maps_eta(origin, destination)
        elif httpmethod == 'POST':
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








