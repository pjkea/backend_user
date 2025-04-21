import json
import boto3
import psycopg2
import logging
import requests
import phonenumbers
from botocore.exceptions import ClientError
from twilio.rest import Client

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

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

# Function to validate address using Google Geocoding API
def validate_address(address):
    secrets = get_secrets()
    try:
        geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={secrets['GOOGLE_MAPS_API_KEY']}"
        response = requests.get(geocode_url)
        data = response.json()

        # Log full API response for debugging
        logger.info(f"Google API Response: {json.dumps(data, indent=2)}")

        if data["status"] == "OK":
            formatted_address = data["results"][0]["formatted_address"]
            location = data["results"][0]["geometry"]["location"]
            latitude, longitude = location["lat"], location["lng"]
            logger.info(f"Address validated: {formatted_address} (Lat: {latitude}, Lng: {longitude})")
            return formatted_address, latitude, longitude
        else:
            logger.warning(f"Invalid address: {address}. Google API Status: {data.get('status')}")
            return None, None, None
    except Exception as e:
        logger.error(f"Google Geocoding API error: {str(e)}", exc_info=True)
        return None, None, None

# Function to send SMS using Twilio
def send_twilio_sms(to_number, message):
    secrets = get_secrets()
    try:
        # Validate phone number format
        parsed_number = phonenumbers.parse(to_number, None)
        if not phonenumbers.is_valid_number(parsed_number):
            logger.error(f"Invalid phone number format: {to_number}")
            return

        formatted_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
        logger.info(f"Sending SMS to: {formatted_number}")

        client = Client(secrets["TWILIO_ACCOUNT_SID"], secrets["TWILIO_AUTH_TOKEN"])
        sms = client.messages.create(
            body=message,
            from_=secrets["TWILIO_PHONE_NUMBER"],
            to=formatted_number
        )
        logger.info(f"Twilio SMS sent successfully to {formatted_number}: {sms.sid}")
    except Exception as e:
        logger.error(f"Twilio SMS sending failed: {str(e)}", exc_info=True)

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
            Subject=subject
        )
        logger.info(f"Logged event to SNS: {message}")
    except Exception as e:
        logger.error(f"SNS logging failed: {str(e)}", exc_info=True)

