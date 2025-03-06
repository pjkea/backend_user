import json
import datetime
import boto3
import logging
import requests
from psycopg2.extras import RealDictCursor
from serviceRequest.layers.utils import get_secrets, get_db_connection, calculate_google_maps_eta


# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Google API Key
GOOGLE_MAPS_API_KEY = secrets["GOOGLE_MAPS_API_KEY"]

# SNS Topics
LOCATION_TRACKING_TOPIC_ARN = secrets["LOCATION_TRACKING_TOPIC_ARN"]


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
        raise e


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        try:
            # Parse request data
            body = json.loads(event.get('body', '{}'))
            http_method = event.get('httpMethod', body.get('httpMethod', ''))
            query_params = event.get('queryStringParameters', body.get('queryStringParameters', {})) or {}

            # Get user and sp ids
            user_id = query_params.get('userid')
            sp_id = query_params.get('tidyspid')

            if not user_id or not sp_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing required parameters'})
                }

            # Get user location
            cursor.execute("""SELECT address, longitude as lng, latitude as lat FROM requests WHERE userid = %s""", (user_id,))
            user_location = cursor.fetchone()

            if not user_location:
                return {
                    'statusCode': 404,
                    'body': json.dumps({'error': 'User location not found'})
                }

            logger.info(f"User location: {user_location}")

        except Exception as e:
            logger.error(f'Couldnt get current location: {e}')

        try:
            # Get service provider location
            if http_method == 'GET' or http_method == 'POST':
                current_location = get_current_location()

                # Calculate ETA
                origin = (current_location['latitude'], current_location['longitude'])
                destination = (user_location['lat'], user_location['lng'])
                eta_info = calculate_google_maps_eta(origin, destination)

                message = {
                    'provider': {
                        'id': sp_id,
                        'currentLocation': current_location,
                    },
                    'destination': {
                        'address': user_location['address'],
                        'latitude': user_location['lat'],
                        'longitude': user_location['lng']
                    },
                    'eta': eta_info,
                    'timestamp': datetime.utcnow().isoformat(),
                    'userid': user_id
                }

                # Publish to SNS
                sns_client.publish(
                    TopicARN=LOCATION_TRACKING_TOPIC_ARN,
                    Message=json.dumps({message}),
                    Subject='TidySP Location',
                )
                logger.info("Succesfully sent Service Provider Loaction")

                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'status': 'success',
                        'message': 'Location and ETA published successfully'
                    })
                }

            else:
                return {
                    'statusCode': 405,
                    'body': json.dumps({'error': 'Invalid http method'})
                }

        except Exception as e:
            logger.error(f'Couldnt get current location: {e}')

    except Exception as e:
        logger.error(f'Error in lambda_handler: {e}')
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

    finally:
        if connection:
            connection.close()
        if cursor:
            cursor.close()
#L1
