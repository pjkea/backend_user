import psycopg2
import os
import json
import boto3
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS Secrets Manager client
secrets_client = boto3.client("secretsmanager")

# Retrieve database credentials from AWS Secrets Manager
def get_db_credentials():
    try:
        secret_name = "tidyzon-env-variables"
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secrets = json.loads(response["SecretString"])
        return {
            "host": secrets["DB_HOST"],
            "database": secrets["DB_NAME"],
            "user": secrets["DB_USER"],
            "password": secrets["DB_PASSWORD"],
            "port": secrets.get("DB_PORT", "5432")
        }
    except Exception as e:
        logger.error(f"Error retrieving database credentials: {str(e)}")
        raise

# Database connection function with try-catch
def get_db_connection():
    try:
        creds = get_db_credentials()
        return psycopg2.connect(
            host=creds["host"],
            database=creds["database"],
            user=creds["user"],
            password=creds["password"],
            port=creds["port"]
        )
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    Lambda function to receive SNS messages from LoggingSNS and store in PostgreSQL.
    """
    try:
        # Parse SNS messages
        for record in event["Records"]:
            sns_message = json.loads(record["Sns"]["Message"])

            message_id = record["Sns"]["MessageId"]
            log_type_id = sns_message.get("logtypeid", 1)  # Default to 'Info'
            category_id = sns_message.get("categoryid", 1)  # Default category
            transaction_type_id = sns_message.get("transactiontypeid", 1)
            status_id = sns_message.get("statusid", 1)
            error_message = sns_message.get("error", None)

            # Connect to PostgreSQL
            conn = get_db_connection()
            cur = conn.cursor()

            # Insert log into the database
            insert_query = """
                INSERT INTO logs.events (logtypeid, categoryid, transactiontypeid, statusid, error_message, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """
            cur.execute(insert_query, (log_type_id, category_id, transaction_type_id, status_id, error_message))

            # Commit transaction
            conn.commit()

            # Close connection
            cur.close()
            conn.close()

            logger.info(f"Log entry saved successfully for Message ID: {message_id}")

        return {"status": "success", "message": "Log entry saved successfully"}

    except Exception as e:
        logger.error(f"Error processing SNS message: {str(e)}")
        return {"status": "error", "message": str(e)}
