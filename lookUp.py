import json
import os
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import datetime  # Ensure datetime is imported

# Initialize AWS clients
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Load secrets from AWS Secrets Manager
secret_name = "tidyzon-env-variables"
secrets = json.loads(secrets_client.get_secret_value(SecretId=secret_name)["SecretString"])

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

# Database connection function with error handling
def get_db_connection():
    try:
        connection = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
        logger.info("Database connection established successfully")
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}", exc_info=True)
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({"logtypeid": 3, "categoryid": 2, "transactiontypeid": 5, "statusid": 2, "error": str(e)}),
            Subject="Lookup - Database Connection Error"
        )
        raise

# Lambda function handler
def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Extract and parse the request body safely
        body = event.get("body")
        if body:
            body = json.loads(body)
        else:
            raise ValueError("Request body is missing")

        table_name = body.get("table_name")
        fields = body.get("fields", ["id", "name"])  # Default fields
        where_conditions = body.get("where", None)
        order_by = body.get("order_by", None)  # No default `id` to avoid issues

        if not table_name:
            raise ValueError("Missing required parameter: table_name")

        # Construct SELECT fields
        fields_str = ", ".join(fields)

        # Construct WHERE clause dynamically
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
        query = f"SELECT {fields_str} FROM {table_name} {where_clause}"
        if order_by:
            query += f" ORDER BY {order_by} ASC"

        logger.info(f"Executing Query: {query} with Values: {values}")

        # Database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, values)
        results = cursor.fetchall()

        # Convert datetime fields to strings
        for row in results:
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
                "table_name": table_name,
                "order_by": order_by if order_by else "None",
                "where_conditions": where_conditions if where_conditions else "None"
            }),
            Subject="Lookup - Success"
        )

        logger.info(f"Query executed successfully for table: {table_name}")

        return {
            "statusCode": 200,
            "body": json.dumps(results)
        }

    except Exception as e:
        logger.error(f"Lookup error: {str(e)}", exc_info=True)

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
