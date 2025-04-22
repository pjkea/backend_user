import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, paginate_query_results

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")
        activity_type = query_params.get("activity_type")  # Optional filter
        date_from = query_params.get("date_from")  # Optional date range start
        date_to = query_params.get("date_to")  # Optional date range end
        page = int(query_params.get("page", 1))
        page_size = int(query_params.get("pagesize", 20))
        is_admin = query_params.get("is_admin") == "true"  # Admin view flag
        target_user_id = query_params.get("target_userid")  # For admin to view specific user

        # Validate required parameters
        if not user_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: userid'})
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Build the query with optional filters
        query = """
            SELECT logid, userid, activity_type, details, ip_address, user_agent, createdat
            FROM user_activity_logs
            WHERE 1=1
        """

        query_params = []

        # Admin view can see any user's logs if specified
        if is_admin and target_user_id:
            query += " AND userid = %s"
            query_params.append(target_user_id)
        # Regular user can only see their own logs
        elif not is_admin:
            query += " AND userid = %s"
            query_params.append(user_id)

        # Apply activity type filter if provided
        if activity_type:
            query += " AND activity_type = %s"
            query_params.append(activity_type)

        # Apply date range filters if provided
        if date_from:
            query += " AND createdat >= %s"
            query_params.append(date_from)

        if date_to:
            query += " AND createdat <= %s"
            query_params.append(date_to)

        # Add ordering
        query += " ORDER BY createdat DESC"

        # Execute paginated query
        paginated_results = paginate_query_results(cursor, query, query_params, page, page_size)

        # Format activity logs for response
        formatted_logs = []
        for log in paginated_results['results']:
            formatted_log = dict(log)

            # Convert datetime objects to ISO strings
            for key, value in formatted_log.items():
                if isinstance(value, datetime):
                    formatted_log[key] = value.isoformat()

            # Parse JSON details if they exist
            if formatted_log.get('details') and isinstance(formatted_log['details'], str):
                try:
                    formatted_log['details'] = json.loads(formatted_log['details'])
                except:
                    # Keep as string if not valid JSON
                    pass

            formatted_logs.append(formatted_log)

        # For admin view, add security alerts if requested
        if is_admin and query_params.get("include_alerts") == "true":
            alert_query = """
                SELECT alertid, userid, alert_type, details, resolved, resolvedby, createdat, resolvedat
                FROM security_alerts
                WHERE resolved = FALSE
                ORDER BY createdat DESC
                LIMIT 50
            """

            cursor.execute(alert_query)
            alerts = cursor.fetchall()

            formatted_alerts = []
            for alert in alerts:
                formatted_alert = dict(alert)

                # Convert datetime objects to ISO strings
                for key, value in formatted_alert.items():
                    if isinstance(value, datetime):
                        formatted_alert[key] = value.isoformat()

                # Parse JSON details if they exist
                if formatted_alert.get('details') and isinstance(formatted_alert['details'], str):
                    try:
                        formatted_alert['details'] = json.loads(formatted_alert['details'])
                    except:
                        # Keep as string if not valid JSON
                        pass

                formatted_alerts.append(formatted_alert)
        else:
            formatted_alerts = []

        # Log success to SNS
        log_to_sns(1, 7, 11, 1, {
            "user_id": user_id,
            "is_admin": is_admin,
            "target_user_id": target_user_id,
            "log_count": len(formatted_logs)
        }, "Get User Activity Logs - Success", user_id)

        logger.info(f"Successfully retrieved activity logs for user {target_user_id or user_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Activity logs retrieved successfully',
                'logs': formatted_logs,
                'alerts': formatted_alerts if is_admin and query_params.get("include_alerts") == "true" else None,
                'pagination': paginated_results['pagination']
            })
        }

    except Exception as e:
        logger.error(f"Failed to retrieve activity logs: {e}")

        # Log error to SNS
        log_to_sns(4, 7, 11, 43, {
            "user_id": user_id,
            "is_admin": is_admin if 'is_admin' in locals() else False,
            "target_user_id": target_user_id if 'target_user_id' in locals() else None,
            "error": str(e)
        }, "Get User Activity Logs - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to retrieve activity logs',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()