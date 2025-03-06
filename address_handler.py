import json
import boto3
import psycopg2
import logging
import requests
from datetime import datetime
from psycopg2.extras import RealDictCursor
from botocore.exceptions import ClientError

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Load secrets from AWS Secrets Manager
secrets = json.loads(secrets_client.get_secret_value(SecretId="tidyzon-env-variables")["SecretString"])

# SNS Logging Topic
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]
SERVICE_REQUEST_TOPIC_ARN = secrets["SERVICE_REQUEST_TOPIC_ARN"]

# Google API Key for Geocoding
GOOGLE_MAPS_API_KEY = secrets["GOOGLE_MAPS_API_KEY"]

# CloudWatch Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Database connection function with try-catch
def get_db_connection():
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
    try:
        geocode_url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={GOOGLE_MAPS_API_KEY}"
        response = requests.get(geocode_url)
        data = response.json()

        if data["status"] == "OK":
            # Extract formatted address and latitude/longitude
            formatted_address = data["results"][0]["formatted_address"]
            location = data["results"][0]["geometry"]["location"]
            latitude, longitude = location["lat"], location["lng"]
            logger.info(f"Address validated: {formatted_address} (Lat: {latitude}, Lng: {longitude})")
            return formatted_address, latitude, longitude
        else:
            logger.warning(f"Invalid address: {address}")
            return None, None, None
    except Exception as e:
        logger.error(f"Google Geocoding API error: {str(e)}", exc_info=True)
        return None, None, None

def addressHandler(event, context):
    connection = None
    cursor = None

    try:
        # Parse request body
        body = json.loads(event.get("body", "{}"))

        # Extract user data
        userid = body.get("userid")
        addressline1 = body.get("addressline1")
        addressline2 = body.get("addressline2", None)  # Optional
        city = body.get("city")
        state = body.get("state")
        postalcode = body.get("postalcode")
        countryid = body.get("countryid")  # Must be a valid country ID
        updated_at = datetime.utcnow()

        # Validate required fields
        if not all([userid, addressline1, city, state, postalcode, countryid]):
            raise ValueError("Missing required fields")

        # Validate address using Google Geocoding API
        full_address = f"{addressline1}, {addressline2 or ''}, {city}, {state}, {postalcode}"
        formatted_address, latitude, longitude = validate_address(full_address)

        if not formatted_address:
            raise ValueError("Invalid address provided")

        # Step 1: Connect to PostgreSQL
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Check if user exists
        cursor.execute("SELECT userid FROM users WHERE userid = %s", (userid,))
        user_exists = cursor.fetchone()
        if not user_exists:
            raise ValueError("User not found")

        # Step 2: Insert or update user address details
        cursor.execute("""
            INSERT INTO userdetails (userid, streetaddress1, streetaddress2, city, state, postalcode, countryid, latitude, longitude, updatedat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (userid)
            DO UPDATE SET
                streetaddress1 = EXCLUDED.streetaddress1,
                streetaddress2 = EXCLUDED.streetaddress2,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                postalcode = EXCLUDED.postalcode,
                countryid = EXCLUDED.countryid,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                updatedat = EXCLUDED.updatedat;
        """, (userid, addressline1, addressline2, city, state, postalcode, countryid, latitude, longitude, updated_at))

        connection.commit()

        # Send address to Service Request Handler (Update)
        sns_client.publish(
            TopicArn=SERVICE_REQUEST_TOPIC_ARN,
            Message=json.dumps({
                "address": formatted_address,
                "longitude": longitude,
                "latitude": latitude,
            }),
            Subject="Address Update",
        )

        # Log success to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 1,
                "categoryid": 11,  # User Address Management
                "transactiontypeid": 12,  # Address Update
                "statusid": 1,  # Success
                "userid": userid,
                "formatted_address": formatted_address,
                "latitude": latitude,
                "longitude": longitude
            }),
            Subject="User Address - Success"
        )

        logger.info(f"User {userid} address validated and saved successfully")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "User address validated and saved successfully",
                "userid": userid,
                "formatted_address": formatted_address,
                "latitude": latitude,
                "longitude": longitude
            })
        }

    except Exception as e:
        logger.error(f"Address update error: {str(e)}", exc_info=True)

        # Log failure to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 3,
                "categoryid": 11,
                "transactiontypeid": 12,
                "statusid": 2,  # Failure
                "error": str(e),
                "userid": userid if 'userid' in locals() else None
            }),
            Subject="User Address - Error"
        )

        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

