import json
import psycopg2
import boto3
import logging

# Initialize AWS services
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")
ses_client = boto3.client("ses", region_name="us-east-1")

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Fetch secrets from Secrets Manager
secrets = json.loads(secrets_client.get_secret_value(SecretId="tidyzon-env-variables")["SecretString"])





