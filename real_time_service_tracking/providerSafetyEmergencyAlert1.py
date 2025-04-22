import json
import boto3
import logging

from datetime import datetime
from psycopg2.extras import RealDictCursor
from layers.utils import get_secrets, get_db_connection

# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topic
EMERGENCY_ALERT_TOPIC_ARN = secrets['EMERGENCY_ALERT_TOPIC_ARN']


def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))

        tidyspid = body.get('tidyspid')
        orderid = body.get('orderid')
        alert_type = body.get('alert_type', 'emergency')
        description = body.get('description', 'Emergency Alert triggered')
        location = body.get('location', {})

        if not tidyspid:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f'Missing required parameter: {tidyspid}'})
            }

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("SELECT userid FROM tidyspid WHERE tidyspid = %s", (tidyspid,))

        provider = cursor.fetchone()

        if not provider:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Provider not found'})
            }

        if orderid:
            cursor.execute("SELECT * FROM orders WHERE tidyspid = %s", (tidyspid,))

            order = cursor.fetchone()

        try:
            cursor.execute("""INSERT INTO tidyspalerts (tidyspid, orderid, alerttype, description, latitude, longitude, status, createdat)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING alertid""",
            (tidyspid,orderid, alert_type, description, location['latitude'], location['longitude'], location['status'], datetime.now()))

            alert = cursor.fetchone()

            if alert:
                alertid = alert[0]
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error recording alert: {str(e)}")

        alert_data = {
            'alertid': alertid,
            'tidyspid': tidyspid,
            'userid': provider.get('userid'),
            'orderid': orderid,
            'alert_type': alert_type,
            'description': description,
            'location': location,
            'time': datetime.utcnow().isoformat(),
        }

        sns_client.publish(
            TopicArn=EMERGENCY_ALERT_TOPIC_ARN,
            Message=json.dumps(alert_data),
            Subject=f'Emergency Alert: {alert_type}'
        )

        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Emergency alert triggered successfully',
                'alert_id': alertid,
                'alert_type': alert_type,
                'status': 'processing'
            })
        }

    except Exception as e:
        logger.error(f"Error processing alert: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Internal server error: {str(e)}'})
        }



