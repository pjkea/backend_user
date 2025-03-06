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

        table_name = "servicecategories"
        fields = body.get("fields", ["categoryid", "categoryname"])
        where_conditions = body.get("where", None)
        order_by = body.get("order_by", None)

        if not table_name:
            raise ValueError("Table name is missing")

        # Construct SELECT fields
        fields = ",".join(fields)

        # Construct WHERE clause
        where_clause = ""
        values = []
        if where_conditions:
            conditions = []
            for key, value in where_conditions.items():
                conditions.append(f"{key} = %s")
                values.append(value)
            where_clause = f"WHERE {' AND '.join(conditions)}"

        # Ensure `order_by` exists in the selected fields
        if order_by and order_by not in fields:
            order_by = None  # Ignore invalid order_by column

        # Construct final SQL query
        query = f"SELECT {fields} FROM {table_name} {where_clause}"
        if order_by:
            query += f" ORDER BY {order_by} ASC"

        logger.info(f"Executing Query: {query} with Values: {values}")

        # Database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, values)
        results = cursor.fetchall()

        # Convert results to a list of dictionaries
        results_list = list(results)

        # Convert datetime fields to strings
        for row in results:
            for key in row:
                if isinstance(row[key], (datetime.datetime, datetime.date)):
                    row[key] = row[key].isoformat()

        logger.info(f"Details successfully retrieved")

        return {
            "statusCode": 200,
            "body": json.dumps(results)
        }

    except Exception as e:
        logger.error(f"Failed to retrieve service details: {e}")

        # Log error event to SNS
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 3,
                "categoryid": 2,
                "transactiontypeid": 5,
                "statusid": 2,
                "error": str(e),
                "table_name": table_name if table_name else "N/A",
                "where_conditions": where_conditions if where_conditions else "None"
            }),
            Subject="Lookup - Error"
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