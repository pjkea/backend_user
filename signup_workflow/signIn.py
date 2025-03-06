import json
import os
import boto3
import psycopg2
from datetime import datetime
from psycopg2.extras import RealDictCursor
from botocore.exceptions import BotoCoreError, ClientError

# Load environment variables from AWS Secrets Manager
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
secrets = json.loads(
    secrets_manager.get_secret_value(SecretId="tidyzon-env-variables")["SecretString"]
)

DB_HOST = secrets["DB_HOST"]
DB_NAME = secrets["DB_NAME"]
DB_USER = secrets["DB_USER"]
DB_PASSWORD = secrets["DB_PASSWORD"]
DB_PORT = secrets["DB_PORT"]
COGNITO_CLIENT_ID = secrets["COGNITO_CLIENT_ID"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]

# Initialize AWS clients
cognito_client = boto3.client("cognito-idp", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Database connection function with error handling
def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
    except Exception as e:
        log_to_sns("SignIn - Database Connection Error", str(e), status_id=2)
        raise Exception("Database connection error")

# Function to log events to SNS
def log_to_sns(subject, error_message, status_id, email=None):
    sns_client.publish(
        TopicArn=SNS_LOGGING_TOPIC_ARN,
        Message=json.dumps({
            "logtypeid": 3,  # Error log
            "categoryid": 10,  # User Authentication
            "transactiontypeid": 12,  # User Sign-In
            "statusid": status_id,  # 1 = Success, 2 = Failure
            "error": error_message,
            "email": email
        }),
        Subject=subject
    )

# Sign-in function
def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse input request
        body = json.loads(event.get("body", "{}"))
        email = body.get("email")
        password = body.get("password")

        if not email or not password:
            raise ValueError("Missing email or password")

        # Authenticate user via Cognito
        try:
            response = cognito_client.initiate_auth(
                ClientId=COGNITO_CLIENT_ID,
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={
                    "USERNAME": email,
                    "PASSWORD": password
                }
            )
            id_token = response["AuthenticationResult"]["IdToken"]
        except cognito_client.exceptions.NotAuthorizedException:
            raise ValueError("Invalid email or password")
        except cognito_client.exceptions.UserNotFoundException:
            raise ValueError("User does not exist")
        except (BotoCoreError, ClientError) as e:
            log_to_sns("SignIn - Cognito Error", str(e), status_id=2, email=email)
            raise Exception("Error authenticating with Cognito")

        # Retrieve user details from the database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            "SELECT userid, username, email, roleid, isactive FROM users WHERE email = %s",
            (email,)
        )
        user = cursor.fetchone()

        if not user:
            raise ValueError("User not found in database")

        if not user["isactive"]:
            raise ValueError("User account is inactive")

        # Log success to SNS
        log_to_sns("SignIn - Success", "User signed in successfully", status_id=1, email=email)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "User signed in successfully",
                "user": {
                    "userid": user["userid"],
                    "username": user["username"],
                    "email": user["email"],
                    "roleid": user["roleid"],
                },
                "token": id_token
            })
        }

    except ValueError as ve:
        log_to_sns("SignIn - Validation Error", str(ve), status_id=2, email=email if "email" in locals() else None)
        return {"statusCode": 400, "body": json.dumps({"error": str(ve)})}

    except Exception as e:
        log_to_sns("SignIn - Error", str(e), status_id=2, email=email if "email" in locals() else None)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

