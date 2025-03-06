import json
import boto3
import logging
import math
from psycopg2.extras import RealDictCursor
from serviceRequest.layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS Topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
ASSIGN_PROVIDER_TOPIC_ARN = secrets["ASSIGN_PROVIDER_TOPIC_ARN"]


def calculate_distance(lat1, lon1, lat2, lon2):
    # Radius of earth in kilometers
    R = 6371.0

    # Convert latitude and longitude to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = (math.sin(dlat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) *
         math.sin(dlon / 2) ** 2)

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Calculate the distance
    distance = R * c

    return distance


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Retrieve service request
        for record in event["Records"]:
            message = json.loads(record["Sns"]["Message"])
            service_details = message.get("servicedetails", {})
            userid = service_details.get("userid")

            # Get user location
            latitude = service_details.get("latitude")
            longitude = service_details.get("longitude")
            radius = 5

            # Establish database connection
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Fetch service provider details
            query = """
            SELECT t.tidyspid, ud.firstname, ud.lastname, ud.latitude, ud.longitude,u.phone_number, u.email 
            FROM tidysp t JOIN "users" u ON t.userid = u.userid JOIN userdetails ud ON u.userid = ud.userid"""
            cursor.execute(query)
            sp_records = cursor.fetchall()

            # Filter records by distance
            nearby_sp = []
            for tidysp in sp_records:
                # Skip records without latitude/longitude
                if not tidysp['latitude'] or not tidysp['longitude']:
                    continue

                # Calculate distance
                distance = calculate_distance(latitude, longitude, tidysp['latitude'], tidysp['longitude'])
                # Check if within max distance
                if distance <= radius:
                    # Add distance to the record
                    sp_within_distance = dict(tidysp)
                    sp_within_distance['distance_km'] = round(distance, 2)
                    nearby_sp.append(sp_within_distance)

            # Sort records by distance
            def sort_by_distance(tidysp):
                return tidysp.get('distance_km', float('inf'))

            nearby_sp.sort(key=sort_by_distance)

            # Send to provider sns
            sns_client.publish(
                TopicArn=ASSIGN_PROVIDER_TOPIC_ARN,
                Message=json.dumps({
                    'total_nearby_records': len(nearby_sp),
                    'max_radius': radius,
                    'center_point': {
                        'latitude': latitude,
                        'longitude': longitude
                    },
                    'available_sp': nearby_sp
                })
            )

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Unexpected system error',
                'message': str(e)
            })
        }

    finally:
        if connection:
            connection.close()
        if cursor:
            cursor.close()










