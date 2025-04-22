import json
import boto3
import psycopg2
import logging
import math
import requests
from datetime import datetime
from botocore.exceptions import ClientError
from twilio.rest import Client

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Load secrets from AWS Secrets Manager
# secrets = json.loads(secrets_client.get_secret_value(SecretId="tidyzon-env-variables")["SecretString"])

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Function to load secrets from AWS Secrets Manager
def get_secrets():
    try:
        secrets = json.loads(secrets_client.get_secret_value(SecretId="tidyzon-env-variables")["SecretString"])
        return secrets
    except ClientError as e:
        logger.error(f"AWS Secrets Manager error: {e.response['Error']['Message']}", exc_info=True)
        return None


# Function to establish a database connection
def get_db_connection():
    secrets = get_secrets()
    try:
        connection = psycopg2.connect(
            host=secrets["DB_HOST"],
            database=secrets["DB_NAME"],
            user=secrets["DB_USER"],
            password=secrets["DB_PASSWORD"],
            port=secrets["DB_PORT"]
        )
        logger.info("Database connection established successfully")
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}", exc_info=True)
        raise


# Function to log events to AWS SNS
def log_to_sns(logtypeid, categoryid, transactiontypeid, statusid, message, subject, userid=None):
    secrets = get_secrets()
    try:
        sns_client.publish(
            TopicArn=secrets["SNS_LOGGING_TOPIC_ARN"],
            Message=json.dumps({
                "logtypeid": logtypeid,
                "categoryid": categoryid,
                "transactiontypeid": transactiontypeid,
                "statusid": statusid,
                "message": message,
                "userid": userid
            }),
            Subject= subject
        )
        logger.info(f"Logged event to SNS: {message}")
    except Exception as e:
        logger.error(f"SNS logging failed: {str(e)}", exc_info=True)


# Function to calculate distance to user
def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in meters using Haversine formula"""
    R = 6371000  # Earth's radius in meters

    # Convert latitude and longitude from degrees to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Haversine formula
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    distance = R * c

    return distance


# Function to calculate estimated time of arrival using google maps api
def calculate_eta_google(origin_lat, origin_lng, dest_lat, dest_lng):
    """Calculate ETA using Google Maps API"""
    secrets = get_secrets()
    GOOGLE_MAPS_API_KEY = secrets['GOOGLE_MAPS_API_KEY']
    url = f"https://maps.googleapis.com/maps/api/directions/json?origin={origin_lat},{origin_lng}&destination={dest_lat},{dest_lng}&mode=driving&departure_time=now&key={GOOGLE_MAPS_API_KEY}"

    response = requests.get(url)
    data = response.json()

    if data['status'] == 'OK' and len(data['routes']) > 0:
        # Extract duration in traffic if available, otherwise regular duration
        leg = data['routes'][0]['legs'][0]
        if 'duration_in_traffic' in leg:
            duration_seconds = leg['duration_in_traffic']['value']
        else:
            duration_seconds = leg['duration']['value']

        # Distance in meters
        distance_meters = leg['distance']['value']

        return {
            'eta_seconds': duration_seconds,
            'eta_text': leg.get('duration_in_traffic', leg['duration'])['text'],
            'distance_meters': distance_meters,
            'distance_text': leg['distance']['text']
        }

    return None
