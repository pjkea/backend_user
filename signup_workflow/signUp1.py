import json
import boto3
import logging
import psycopg2
from datetime import datetime
from layers.utils import get_secrets

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# CloudWatch logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load secrets
secrets = get_secrets()

# SNS Topics
SIGNUP_SNS_TOPIC_ARN = secrets["SIGNUP_SNS_TOPIC_ARN"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]

def signUpValidate(event, context):

    try:
        # Parse request body
        body = json.loads(event.get("body", "{}"))

        # Extract user data
        firstname = body.get("firstname")
        lastname = body.get("lastname")
        email = body.get("email")
        phone_number = body.get("phone_number")
        password = body.get("password")
        preferred_language = body.get("preferred_language", 1)  # Default language
        roleid = body.get("roleid", 2)  # Default user role
        created_at = datetime.utcnow()
        updated_at = created_at

        if not all([firstname, lastname, email, phone_number, password]):
            raise ValueError("Missing required fields")

        # Send user details to SNS
        sns_client.publish(
            TopicArn=SIGNUP_SNS_TOPIC_ARN,
            Message=json.dumps({
                "firstname": firstname,
                "lastname": lastname,
                "email": email,
                "phone_number": phone_number,
                "password": password,
                "preferred_language": preferred_language,
                "role": roleid,
                "createdat": created_at,
                "updatedat": updated_at,
                "timestamp": created_at.isoformat()
            }),
            Subject="User details"
        )

        # Log success to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 1,
                "catgoryid": 10,
                "transactiontypeid": 11,
                "statusid": 1,
                "email": "",
                "phone_number": ""
            }),
            Subject = "Details sent successfully"
        )

        logger.info("User details sent successfully")

        return {
            "statusCode": 202,
            "body": json.dumps({
                "message": "Your signup request is being processed"
            })
        }

    except Exception as e:
        logger.error(f"User details failed to send: {str(e)}")
        error_message = str(e)

        # Log failure to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 4,
                "catgoryid": 10,
                "transactiontypeid": 11,
                "statusid": 2,
                "error": error_message,
                "email": email,
                "phone_number": phone_number
            }),
            Subject = "User details failed to send"
        )

        return {"statusCode": 400, "body": json.dumps({"error": error_message})}



