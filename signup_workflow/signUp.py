import json
import boto3
import psycopg2
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor
from botocore.exceptions import ClientError
from twilio.rest import Client
from layers.utils import get_db_connection, send_twilio_sms, get_secrets

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")
cognito_client = boto3.client("cognito-idp", region_name="us-east-1")

# Load secrets from AWS Secrets Manager
secrets = get_secrets()

# Cognito Config
USER_POOL_ID = secrets["COGNITO_CLIENT_ID"]
CLIENT_ID = secrets["COGNITO_CLIENT_ID"]

# SNS Topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]

# Twilio Config
TWILIO_ACCOUNT_SID = secrets["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH_TOKEN = secrets["TWILIO_AUTH_TOKEN"]
TWILIO_PHONE_NUMBER = secrets["TWILIO_PHONE_NUMBER"]

# CloudWatch Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def signUpHandler(event, context):
    connection = None
    cursor = None

    try:
        # Parse SNS message
        sns_message = event['Records'][0]['Sns']['Message']
        user_data = json.loads(sns_message)

        # Extract user data from SNS message
        firstname = user_data.get("firstname")
        lastname = user_data.get("lastname")
        email = user_data.get("email")
        phone_number = user_data.get("phone_number")
        password = user_data.get("password")
        preferred_language = user_data.get("preferred_language", 1)  # Default language
        roleid = user_data.get("role", 1)
        created_at = datetime.fromisoformat(user_data.get("createdat"))
        updated_at = datetime.fromisoformat(user_data.get("updatedat"))

        if not all([firstname, lastname, email, phone_number, password]):
            raise ValueError("Missing required fields from SNS message")

        # Step 1: Register the user in Cognito
        response = cognito_client.sign_up(
            ClientId=CLIENT_ID,
            Username=email,
            Password=password,
            UserAttributes=[
                {"Name": "email", "Value": email},
                {"Name": "phone_number", "Value": phone_number},
                {"Name": "given_name", "Value": firstname},
                {"Name": "family_name", "Value": lastname}
            ]
        )

        # Step 2: Store the user in PostgreSQL
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Insert into users table
        cursor.execute("""
            INSERT INTO users (username, email, passwordhash, roleid, preferredlanguage, isactive, createdat, updatedat)
            VALUES (%s, %s, %s, %s, %s, TRUE, %s, %s) RETURNING userid
        """, (email, email, "COGNITO_MANAGED", roleid, preferred_language, created_at, updated_at))

        user_id = cursor.fetchone()["userid"]

        # Insert into userdetails table
        cursor.execute("""
            INSERT INTO userdetails (userid, firstname, lastname, phonenumber, isemailverified, isphoneverified, createdat, updatedat)
            VALUES (%s, %s, %s, %s, FALSE, FALSE, %s, %s)
        """, (user_id, firstname, lastname, phone_number, created_at, updated_at))

        connection.commit()

        # Step 3: Send Twilio SMS to confirm opt-in
        opt_in_message = f"Hello {firstname}, this is Tidyzon, you have successfully signed up for Tidyzon Service. Reply STOP to unsubscribe."
        send_twilio_sms(phone_number, opt_in_message)

        # Log success to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 1,
                "categoryid": 10,  # User Management
                "transactiontypeid": 11,  # User Signup
                "statusid": 1,  # Success
                "userid": user_id,
                "email": email,
                "phone_number": phone_number
            }),
            Subject="User Signup - Success"
        )

        logger.info(f"User {user_id} registered successfully")

        return {
            "statusCode": 201,
            "body": json.dumps({
                "message": "User registered successfully. Verify OTP to continue.",
                "userid": user_id
            })
        }

    except cognito_client.exceptions.UsernameExistsException:
        logger.error(f"User already exists: {email}")
        return {"statusCode": 400, "body": json.dumps({"error": "User already exists"})}

    except Exception as e:
        logger.error(f"Signup error: {str(e)}", exc_info=True)

        # Log failure to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 3,
                "categoryid": 10,
                "transactiontypeid": 11,
                "statusid": 2,  # Failure
                "error": str(e),
                "email": email,
                "phone_number": phone_number
            }),
            Subject="User Signup - Error"
        )

        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

#L2