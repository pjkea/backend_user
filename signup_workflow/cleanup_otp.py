import psycopg2
import os
import json
import boto3

# Initialize SNS client
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Load secrets from AWS Secrets Manager
secrets = json.loads(secrets_client.get_secret_value(SecretId="tidyzon-env-variables")["SecretString"])

# SNS Topic ARN (Replace with your actual ARN)
SNS_TOPIC_ARN = secrets["SNS_TOPIC_ARN"]

def lambda_handler(event, context):
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(
            host=os.environ['DB_HOST'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )
        cursor = connection.cursor()

        # Run cleanup function
        cursor.execute("CALL cleanup_expired_otps();")
        connection.commit()

        # Publish success message to SNS
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 1,  # Info Log
                "categoryid": 4,  # Database Cleanup
                "transactiontypeid": 5,  # OTP Cleanup
                "statusid": 1,  # Success
                "message": "Monthly OTP cleanup executed successfully"
            }),
            Subject="OTP Cleanup - Success"
        )

        return {"statusCode": 200, "body": "Monthly OTP cleanup successful"}

    except Exception as e:
        # Publish failure message to SNS
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 3,  # Error Log
                "categoryid": 4,
                "transactiontypeid": 5,
                "statusid": 2,  # Failure
                "error": str(e)
            }),
            Subject="OTP Cleanup - Error"
        )

        return {"statusCode": 500, "body": str(e)}

    finally:
        cursor.close()
        connection.close()

