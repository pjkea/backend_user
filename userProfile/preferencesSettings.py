import json
import boto3
import logging
from datetime import datetime
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns

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
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")
        http_method = event.get("httpMethod", "GET")

        # Validate required parameters
        if not user_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: userid'})
            }

        # Connect to database
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify user exists
        cursor.execute("""
            SELECT userid FROM users WHERE userid = %s
        """, (user_id,))

        if not cursor.fetchone():
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'User not found'})
            }

        # Handle GET request (fetch preferences)
        if http_method == "GET":
            cursor.execute("""
                SELECT 
                    userid,
                    language,
                    notification_email,
                    notification_sms,
                    notification_push,
                    theme,
                    other_preferences,
                    createdat,
                    updatedat
                FROM user_preferences
                WHERE userid = %s
            """, (user_id,))

            preferences = cursor.fetchone()

            if not preferences:
                # Return default preferences if none exist
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'No preferences found, returning defaults',
                        'preferences': {
                            'language': 'en',
                            'notification_email': True,
                            'notification_sms': True,
                            'notification_push': True,
                            'theme': 'light',
                            'other_preferences': {}
                        }
                    })
                }

            # Format preferences for response
            formatted_preferences = dict(preferences)

            # Convert datetime objects to ISO strings
            for key, value in formatted_preferences.items():
                if isinstance(value, datetime):
                    formatted_preferences[key] = value.isoformat()

            # Parse JSON field if present
            if formatted_preferences.get('other_preferences'):
                if isinstance(formatted_preferences['other_preferences'], str):
                    formatted_preferences['other_preferences'] = json.loads(
                        formatted_preferences['other_preferences'])


            # Log success to SNS
            log_to_sns(1, 7, 2, 1, {"user_id": user_id}, "Get User Preferences - Success", user_id)

            logger.info(f"Successfully retrieved preferences for user {user_id}")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Preferences retrieved successfully',
                    'preferences': formatted_preferences
                })
            }

        elif http_method in ["POST", "PUT"]:
            body = json.loads(event.get('body', '{}'))

            # Extract preferences from body
            language = body.get('language')
            notification_email = body.get('notification_email')
            notification_sms = body.get('notification_sms')
            notification_push = body.get('notification_push')
            theme = body.get('theme')
            other_preferences = body.get('other_preferences')

            # Check if preferences already exist
            cursor.execute("""
                SELECT userid FROM user_preferences WHERE userid = %s
            """, (user_id,))

            preferences_exist = cursor.fetchone() is not None

            # Begin transaction
            connection.autocommit = False

            if preferences_exist:
                # Update existing preferences
                update_fields = []
                update_values = []

                if language is not None:
                    update_fields.append("language = %s")
                    update_values.append(language)

                if notification_email is not None:
                    update_fields.append("notification_email = %s")
                    update_values.append(notification_email)

                if notification_sms is not None:
                    update_fields.append("notification_sms = %s")
                    update_values.append(notification_sms)

                if notification_push is not None:
                    update_fields.append("notification_push = %s")
                    update_values.append(notification_push)

                if theme is not None:
                    update_fields.append("theme = %s")
                    update_values.append(theme)

                if other_preferences is not None:
                    update_fields.append("other_preferences = %s")
                    if isinstance(other_preferences, (dict, list)):
                        update_values.append(json.dumps(other_preferences))
                    else:
                        update_values.append(other_preferences)

                if update_fields:
                    update_fields.append("updatedat = NOW()")
                    query = f"""
                        UPDATE user_preferences 
                        SET {', '.join(update_fields)} 
                        WHERE userid = %s
                    """
                    cursor.execute(query, update_values + [user_id])
            else:
                # Insert new preferences
                insert_fields = ['userid']
                insert_values = [user_id]
                insert_placeholders = ['%s']

                if language is not None:
                    insert_fields.append('language')
                    insert_values.append(language)
                    insert_placeholders.append('%s')

                if notification_email is not None:
                    insert_fields.append('notification_email')
                    insert_values.append(notification_email)
                    insert_placeholders.append('%s')

                if notification_sms is not None:
                    insert_fields.append('notification_sms')
                    insert_values.append(notification_sms)
                    insert_placeholders.append('%s')

                if notification_push is not None:
                    insert_fields.append('notification_push')
                    insert_values.append(notification_push)
                    insert_placeholders.append('%s')

                if theme is not None:
                    insert_fields.append('theme')
                    insert_values.append(theme)
                    insert_placeholders.append('%s')

                if other_preferences is not None:
                    insert_fields.append('other_preferences')
                    if isinstance(other_preferences, (dict, list)):
                        insert_values.append(json.dumps(other_preferences))
                    else:
                        insert_values.append(other_preferences)
                    insert_placeholders.append('%s')

                query = f"""
                    INSERT INTO user_preferences 
                    ({', '.join(insert_fields)}, createdat) 
                    VALUES ({', '.join(insert_placeholders)}, NOW())
                """
                cursor.execute(query, insert_values)

            # Log the preference change in the activity logs
            cursor.execute("""
                INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
                user_id,
                'PREFERENCES_UPDATE',
                json.dumps({
                    'updated_preferences': {k: v for k, v in body.items() if v is not None}
                }),
                event.get('requestContext', {}).get('identity', {}).get('sourceIp', 'unknown')
            ))

            # Commit the transaction
            connection.commit()

            # Log success to SNS
            log_to_sns(1, 7, 3, 1, {
                "user_id": user_id,
                "updated_preferences": [k for k, v in body.items() if v is not None]
            }, "Update User Preferences - Success", user_id)

            logger.info(f"Successfully updated preferences for user {user_id}")

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Preferences updated successfully',
                    'updated_preferences': [k for k, v in body.items() if v is not None]
                })
            }

        # Handle unsupported HTTP methods
        else:
            return {
                'statusCode': 405,
                'body': json.dumps({'message': 'Method not allowed'})
            }

    except Exception as e:
        logger.error(f"Failed to process preferences request: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 3, 43, {
            "user_id": user_id,
            "http_method": http_method if 'http_method' in locals() else None,
            "error": str(e)
        }, "User Preferences - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process preferences request',
                'error': str(e)
            })
        }

    finally:
        if connection:
            connection.autocommit = True
        if cursor:
            cursor.close()
        if connection:
            connection.close()