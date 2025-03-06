import json
import boto3
import logging
from psycopg2.extras import RealDictCursor
from serviceRequest.layers.utils import get_db_connection, get_secrets

# Initialize AWS Clients
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load secrets
secrets = get_secrets()

# SNS Topics
CREATE_SERVICE_REQUEST_TOPIC_ARN = secrets["CREATE_SERVICE_REQUEST_TOPIC_ARN"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        for record in event["Records"]:
            # Gather service details: package, price, location
            message = json.loads(record["Sns"]["Message"])
            userid = message.get("user_id")
            package = message.get("package")
            add_ons = message.get("add_ons", None) # Default if user doesn't choose any add ons
            price = message.get("price")
            address = message.get("address")
            longitude = message.get("longitude")
            latitude = message.get("latitude")

            if not all([userid, package, price, add_ons, address, longitude, latitude]):
                raise ValueError("Missing fields")

            # Service details
            service_details = {
                "userid": userid,
                "package": package,
                "add_ons": add_ons,
                "price": price,
                "address": address,
                "longitude": longitude,
                "latitude": latitude
            }

            # Store in database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            cursor.execute("""
            INSERT INTO requests (userid, package, price, add_ons, address, longitude, latitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (userid, package, price, add_ons, address, longitude, latitude))

            connection.commit()

            # Send to provider selection function
            sns_client.publish(
                TopicArn=CREATE_SERVICE_REQUEST_TOPIC_ARN,
                Message=json.dumps({
                    "userid": userid,
                    "package": package,
                    "price": price,
                    "add_ons": add_ons,
                    "address": address,
                    "longitude": longitude,
                    "latitude": latitude,
                    "service_details": service_details
                }),
                Subject="Service Request"
            )

            # Log success to SNS
            sns_client.publish(
                TopicArn=SNS_LOGGING_TOPIC_ARN,
                Message=json.dumps({
                    "logtypeid": 1,
                    "categoryid": 1,  # Service Request
                    "transactiontypeid": 12,  # Address Update
                    "statusid": 9,  # Service Request Created
                    "userid": userid
                }),
                Subject="Service Request - Success",
            )

            logger.info("Successfully sent request")

            return {
                "statusCode": 200,
                "body": json.dumps({
                    'message': 'Service request created successfully',
                    'provider': 'Your request is being processed. Provider matching typically takes 5-10 minutes.'
                })
            }

    except Exception as e:
        logger.error(f"Failed to send request: {e}")

        # Log failure to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 4,
                "categoryid": 1,  # Service Request
                "transactiontypeid": 12,  # Address Update
                "statusid": 4,  # Service Request Failed
                "userid": userid,
            }),
            Subject="Service Request - Failed",
        )


        return {"statusCode": 500, "error": f"Error processing request: {e}"}

    finally:
        if connection:
            connection.close()
        if cursor:
            cursor.close()





