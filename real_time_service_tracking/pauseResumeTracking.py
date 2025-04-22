import json
import boto3
import logging
import time

from psycopg2.extras import RealDictCursor, Json
from layers.utils import get_secrets, get_db_connection, calculate_eta_google

# Initialize AWS services
secrets_manager = boto3.client('secretsmanager', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))

        trackingid = body.get('trackingid')
        tidyspid = body.get('tidyspid')
        action = body.get('action')
        timestamp = body.get('timestamp', int(time.time()))

        if not trackingid or not tidyspid:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required fields: trackingId, providerId, or action'
                })
            }

        if action not in ['pause', 'resume']:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Invalid action. Must be "pause" or "resume"'
                })
            }
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        if action == 'pause':
            cursor.execute("""UPDATE tracking SET is_paused = %s, status = %s, updatedat = %s WHERE trackingid = %s""",
                           (True, 'Inactive', timestamp, trackingid))

            paused = cursor.fetchone()
            connection.commit()

            if paused is None:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'message': f'Tracking session {trackingid} not found'
                    })
                }

            response = {
                'trackingid': trackingid,
                'tidyspid': tidyspid,
                'timestamp': timestamp,
                'message': f'Tracking {action}d successfully'
            }
            logger.info(f"Successfully {action}d tracking session {trackingid}")

            return {
                'statusCode': 200,
                'body': json.dumps(response)
            }

        else:
            cursor.execute("""UPDATE tracking SET is_paused = %s, status = %s, updatedat = %s WHERE trackingid = %s""",
                           (False, 'Active', timestamp, trackingid))
            paused = cursor.fetchone()
            connection.commit()
            if paused is None:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'message': f'Tracking session {trackingid} not found'
                    })
                }
            response = {
                'trackingid': trackingid,
                'tidyspid': tidyspid,
                'timestamp': timestamp,
                'message': f'Tracking {action}d successfully'
            }
            logger.info(f"Successfully {action}d tracking session {trackingid}")
            return {
                'statusCode': 200,
                'body': json.dumps({response})
            }

    except Exception as e:
        logger.error(f"Error in pause/resume tracking: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Internal server error: {str(e)}'
            })
        }

