import json
import boto3
import logging
import re
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


def validate_email(email):
    """Validate email format"""
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_regex, email) is not None


def validate_phone_number(phone):
    """Validate phone number format"""
    phone_regex = r'^\+?[0-9]{10,15}$'
    return re.match(phone_regex, phone) is not None


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Parse query parameters and body
        query_params = event.get("queryStringParameters", {}) or {}
        user_id = query_params.get("userid")

        body = json.loads(event.get('body', '{}'))

        # Extract update fields from body
        email = body.get('email')
        first_name = body.get('firstname')
        last_name = body.get('lastname')
        phone_number = body.get('phonenumber')
        address = body.get('address')
        city = body.get('city')
        state = body.get('state')
        zipcode = body.get('zipcode')
        country = body.get('country')

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

        # Validate email format if provided
        if email:
            if not validate_email(email):
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Invalid email format'})
                }

            # Check if email is already in use by another user
            cursor.execute("""
                SELECT userid FROM users WHERE email = %s AND userid != %s
            """, (email, user_id))

            if cursor.fetchone():
                return {
                    'statusCode': 409,
                    'body': json.dumps({'message': 'Email is already in use by another user'})
                }

        # Validate phone number format if provided
        if phone_number and not validate_phone_number(phone_number):
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid phone number format'})
            }

        # Begin transaction
        connection.autocommit = False

        # Update email in users table if provided
        if email:
            cursor.execute("""
                UPDATE users SET email = %s, updatedat = NOW() WHERE userid = %s
            """, (email, user_id))

        # Check if user details already exist
        cursor.execute("""
            SELECT userid FROM userdetails WHERE userid = %s
        """, (user_id,))

        user_details_exist = cursor.fetchone() is not None

        # Update or insert userdetails
        user_details_fields = []
        user_details_values = []

        if first_name is not None:
            user_details_fields.append("firstname = %s")
            user_details_values.append(first_name)

        if last_name is not None:
            user_details_fields.append("lastname = %s")
            user_details_values.append(last_name)

        if phone_number is not None:
            user_details_fields.append("phonenumber = %s")
            user_details_values.append(phone_number)

        if address is not None:
            user_details_fields.append("address = %s")
            user_details_values.append(address)

        if city is not None:
            user_details_fields.append("city = %s")
            user_details_values.append(city)

        if state is not None:
            user_details_fields.append("state = %s")
            user_details_values.append(state)

        if zipcode is not None:
            user_details_fields.append("zipcode = %s")
            user_details_values.append(zipcode)

        if country is not None:
            user_details_fields.append("country = %s")
            user_details_values.append(country)

        if user_details_fields:
            if user_details_exist:
                # Update existing userdetails
                user_details_fields.append("updatedat = NOW()")
                query = f"""
                    UPDATE userdetails 
                    SET {', '.join(user_details_fields)} 
                    WHERE userid = %s
                """
                cursor.execute(query, user_details_values + [user_id])
            else:
                # Insert new userdetails
                fields = ['userid'] + [field.split(' = ')[0] for field in user_details_fields if
                                       'updatedat' not in field]
                placeholders = ['%s'] * len(fields)

                query = f"""
                    INSERT INTO userdetails ({', '.join(fields)}, createdat) 
                    VALUES ({', '.join(placeholders)}, NOW())
                """
                cursor.execute(query, [user_id] + user_details_values)

        # Log the user update in the activity logs
        cursor.execute("""
            INSERT INTO user_activity_logs (userid, activity_type, details, ip_address, createdat)
            VALUES (%s, %s, %s, %s, NOW())
        """, (
            user_id,
            'PROFILE_UPDATE',
            json.dumps({
                'updated_fields': {k: v for k, v in body.items() if v is not None}
            }),
            event.get('requestContext', {}).get('identity', {}).get('sourceIp', 'unknown')
        ))

        # Commit the transaction
        connection.commit()

        # Log success to SNS
        log_to_sns(1, 7, 3, 1, {
            "user_id": user_id,
            "updated_fields": [k for k, v in body.items() if v is not None]
        }, "Update User Information - Success", user_id)

        logger.info(f"Successfully updated information for user {user_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'User information updated successfully',
                'updated_fields': [k for k, v in body.items() if v is not None]
            })
        }

    except Exception as e:
        logger.error(f"Failed to update user information: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 3, 43, {"user_id": user_id, "error": str(e)}, "Update User Information - Failed", user_id)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to update user information',
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