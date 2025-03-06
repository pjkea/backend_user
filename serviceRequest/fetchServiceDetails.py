import json
import boto3
import logging
import datetime
from psycopg2.extras import RealDictCursor
from serviceRequest.layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets from Secrets Manager
secrets = get_secrets()

# Extract database credentials
DB_HOST = secrets["DB_HOST"]
DB_NAME = secrets["DB_NAME"]
DB_USER = secrets["DB_USER"]
DB_PASSWORD = secrets["DB_PASSWORD"]
DB_PORT = secrets["DB_PORT"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        body = event.get("body")
        if body:
            body = json.loads(body)
        else:
            raise ValueError("Request body is missing")

        category_id = body.get("categoryid")
        if not category_id:
            raise ValueError("Missing required parameter: category_id")

        # Determine which ID type was provided
        id_type = body.get("id_type", "categoryid")

        # Database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Initialize result structure
        result = {
            "subcategories": [],
            "options": [],
            "products": []
        }

        # Get subcategories based on the selected ID
        subcategoryquery = """
                    SELECT * FROM servicesubcategories 
                    WHERE categoryid = %s
                    ORDER BY subcategoryname ASC
                """

        cursor.execute(subcategoryquery, [category_id])
        subcategories = cursor.fetchall()
        result["subcategories"] = list(subcategories)

        # Get list of subcategory IDs for further queries
        subcategory_ids = []
        if id_type == "categoryid":
            subcategory_ids = [sub["subcategoryid"] for sub in subcategories]
        else:
            subcategory_ids = [category_id]

        # Get service options for these subcategories
        if subcategory_ids:
            placeholders = ','.join(['%s'] * len(subcategory_ids))
            options_query = f"""
                        SELECT * FROM serviceoptions 
                        WHERE subcategoryid IN ({placeholders})
                        ORDER BY optionname ASC
                    """
            cursor.execute(options_query, subcategory_ids)
            options = cursor.fetchall()
            result["options"] = list(options)

        # Query 3: Get service products for these subcategories
        if subcategory_ids:
            placeholders = ','.join(['%s'] * len(subcategory_ids))
            products_query = f"""
                    SELECT * FROM serviceproducts 
                    WHERE subcategoryid IN ({placeholders})
                    ORDER BY productname ASC
                """
            cursor.execute(products_query, subcategory_ids)
            products = cursor.fetchall()
            result["products"] = list(products)

        logger.info(f"Retrieved service details for {category_id}")

        # Convert datetime fields to strings in each result list
        for category in result.keys():
            for row in result[category]:
                for key in row:
                    if isinstance(row[key], (datetime.datetime, datetime.date)):
                        row[key] = row[key].isoformat()

        # Log success event to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 1,
                "categoryid": 2,
                "transactiontypeid": 5,
                "statusid": 1,
                "category_id": category_id
            }),
            Subject="Retrieval Success"
        )

        logger.info(f"Query executed successfully for category id: {category_id}")

        logger.info(f"Details successfully retrieved")

        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }

    except Exception as e:
        logger.error(f"Failed to retrieve related service details: {str(e)}")

        # Log error event to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 3,
                "categoryid": 2,
                "transactiontypeid": 5,
                "statusid": 2,
                "error": str(e),
                "category_id": category_id if 'category_id' in locals() else "N/A",
                "id_type": id_type if 'id_type' in locals() else "N/A"
            }),
            Subject="Retrieval Error"
        )

        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
    }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


