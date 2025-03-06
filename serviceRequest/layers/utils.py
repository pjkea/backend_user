import json
import boto3
import psycopg2
import logging
import requests
from datetime import datetime
from botocore.exceptions import ClientError
from twilio.rest import Client

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")
ses_client = boto3.client("ses", region_name="us-east-1")

# Load secrets from AWS Secrets Manager
secrets = json.loads(secrets_client.get_secret_value(SecretId="tidyzon-env-variables")["SecretString"])

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Google API Key
GOOGLE_MAPS_API_KEY = secrets["GOOGLE_MAPS_API_KEY"]

# Twilio credentials
TWILIO_ACCOUNT_SID = secrets.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = secrets.get("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = secrets.get("TWILIO_PHONE_NUMBER")

# Initialize Twilio Client
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

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


def send_sms_via_twilio(phone_number, message):
    """Sends an SMS using Twilio"""
    try:
        # Ensure phone number is in E.164 format
        if not phone_number.startswith('+'):
            phone_number = f"+{phone_number}"

        twilio_message = twilio_client.messages.create(
            body=message,
            from_=TWILIO_PHONE_NUMBER,
            to=phone_number
        )
        logger.info(f"SMS sent via Twilio: {twilio_message.sid}")
        return "SMS Sent via Twilio"
    except Exception as twilio_error:
        logger.warning(f"Twilio SMS failed: {twilio_error}")
        raise


def send_email_via_ses(email, subject, message):
    """Sends an email using AWS SES"""
    try:
        # Configurable sender email
        sender_email = "notifications@yourdomain.com"

        # Create HTML message with proper formatting
        html_message = message.replace('\n', '<br>')

        # Send email
        ses_client.send_email(
            Source=sender_email,
            Destination={"ToAddresses": [email]},
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Text": {"Data": message},
                    "Html": {"Data": f"<p>{html_message}</p>"}
                }
            }
        )

        logger.info(f"Email sent to {email}")
        return "Email Sent Successfully"
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise