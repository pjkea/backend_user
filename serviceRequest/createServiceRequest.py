import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor
from serviceRequest.layers.utils import get_db_connection, get_secrets, log_to_sns

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
            calculation = message.get("calculation", {})

            package_info = calculation.get("package", {})
            add_ons = calculation.get("addOns", [])
            price_detail = calculation.get("priceDetail", {})

            address = message.get("address")
            longitude = message.get("longitude")
            latitude = message.get("latitude")

            is_immediate = message.get("is_immediate", True)  # Default to immediate service
            datetime = None

            if is_immediate:
                datetime = datetime.datetime.now().isoformat()
            else:
                datetime = message.get("scheduled_datetime")

                if not datetime:
                    raise ValueError("Scheduled date and time is required for non-immediate service")

            if not all([userid, package_info, price_detail, address, longitude, latitude]):
                raise ValueError("Missing required fields")

            # Service details
            service_details = {
                'userid': userid,
                'package': json.dumps(package_info),
                'add_ons': json.dumps(add_ons),
                'price': json.dumps(price_detail),
                'address': address,
                'longitude': longitude,
                'latitude': latitude,
                'datetime': datetime,
            }

            # Store in database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            cursor.execute("""
            INSERT INTO requests (userid, package, price, add_ons, address, longitude, latitude, scheduledfor)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (userid, service_details["package"], service_details["price"], add_ons, address, longitude, latitude, datetime))

            result = cursor.fetchone()
            requestid = result['requestid'] if result else None

            connection.commit()

            # Send to provider selection function
            sns_client.publish(
                TopicArn=CREATE_SERVICE_REQUEST_TOPIC_ARN,
                Message=json.dumps({
                    'userid': userid,
                    'requestid': requestid,
                    'servicedetails': {
                        'userid': userid,
                        'package': package_info,
                        'price': price_detail,
                        'add_ons': add_ons,
                        'address': address,
                        'longitude': longitude,
                        'latitude': latitude,
                        'datetime': datetime
                    }
                }),
                Subject="Service Request"
            )

            # Log success to SNS
            log_to_sns(1, 1, 12, 9, {service_details}, "Service Request - Success", userid)

            logger.info("Successfully sent request")

            return {
                "statusCode": 200,
                "body": json.dumps({
                    'message': 'Service request created successfully',
                    'provider': 'Your request is being processed. Provider matching typically takes 5-10 minutes.',
                    'requestDetails': {
                        'package': package_info["name"],
                        'totalPrice': price_detail["totalPrice"],
                        'currency': price_detail["currency"]
                    }
                })
            }

    except Exception as e:
        logger.error(f"Failed to send request: {e}")

        # Log failure to SNS
        log_to_sns(4, 1, 12, 4, {service_details}, "Service Request - Error", userid)

        return {"statusCode": 500, "error": f"Error processing request: {e}"}

    finally:
        if connection:
            connection.close()
        if cursor:
            cursor.close()
