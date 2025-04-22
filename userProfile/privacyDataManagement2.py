import json
import boto3
import logging
import io
import csv
import time
import zipfile
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

from layers.utils import get_secrets, get_db_connection, log_to_sns, send_email_via_ses

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')
s3_client = boto3.client('s3', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]

# S3 bucket for data exports and deletion records
DATA_EXPORTS_BUCKET = secrets["DATA_EXPORTS_BUCKET"]


def export_user_data(cursor, user_id):
    """Export all user data as CSV files"""
    data_files = {}

    # Export user profile
    cursor.execute("""
        SELECT u.*, ud.*
        FROM users u
        LEFT JOIN userdetails ud ON u.userid = ud.userid
        WHERE u.userid = %s
    """, (user_id,))

    user_profile = cursor.fetchone()

    if user_profile:
        # Create CSV for user profile
        output = io.StringIO()
        writer = csv.writer(output)

        # Write headers
        writer.writerow(user_profile.keys())
        # Write data
        writer.writerow(user_profile.values())

        data_files['user_profile.csv'] = output.getvalue()

    # Export user preferences
    cursor.execute("""
        SELECT *
        FROM user_preferences
        WHERE userid = %s
    """, (user_id,))

    preferences = cursor.fetchall()

    if preferences:
        output = io.StringIO()
        writer = csv.writer(output)

        # Write headers
        writer.writerow(preferences[0].keys())
        # Write data
        for pref in preferences:
            writer.writerow(pref.values())

        data_files['user_preferences.csv'] = output.getvalue()

    # Export payment methods (masked)
    cursor.execute("""
        SELECT paymentsourceid, paymenttype, cardlast4, expirationdate, cardholdername, 
               isdefault, createdat, updatedat
        FROM userpaymentsources
        WHERE userid = %s
    """, (user_id,))

    payment_methods = cursor.fetchall()

    if payment_methods:
        output = io.StringIO()
        writer = csv.writer(output)

        # Write headers
        writer.writerow(payment_methods[0].keys())
        # Write data
        for method in payment_methods:
            writer.writerow(method.values())

        data_files['payment_methods.csv'] = output.getvalue()

    # Export orders
    cursor.execute("""
        SELECT o.*, od.add_ons, od.notes
        FROM orders o
        LEFT JOIN orderdetails od ON o.orderid = od.orderid
        WHERE o.userid = %s
    """, (user_id,))

    orders = cursor.fetchall()

    if orders:
        output = io.StringIO()
        writer = csv.writer(output)

        # Write headers
        writer.writerow(orders[0].keys())
        # Write data
        for order in orders:
            writer.writerow(order.values())

        data_files['orders.csv'] = output.getvalue()

    # Export reviews
    cursor.execute("""
        SELECT *
        FROM reviews
        WHERE userid = %s
    """, (user_id,))

    reviews = cursor.fetchall()

    if reviews:
        output = io.StringIO()
        writer = csv.writer(output)

        # Write headers
        writer.writerow(reviews[0].keys())
        # Write data
        for review in reviews:
            writer.writerow(review.values())

        data_files['reviews.csv'] = output.getvalue()

    # Export user activity logs
    cursor.execute("""
        SELECT *
        FROM user_activity_logs
        WHERE userid = %s
        ORDER BY createdat DESC
    """, (user_id,))

    activity_logs = cursor.fetchall()

    if activity_logs:
        output = io.StringIO()
        writer = csv.writer(output)

        # Write headers
        writer.writerow(activity_logs[0].keys())
        # Write data
        for log in activity_logs:
            writer.writerow(log.values())

        data_files['activity_logs.csv'] = output.getvalue()

    # Create a README file with export information
    readme = f"""
    Data Export for User ID: {user_id}
    Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

    This export contains the following files:

    """

    for filename in data_files.keys():
        readme += f"- {filename}\n"

    readme += """
    For questions about this data export, please contact our support team.
    """

    data_files['README.txt'] = readme

    return data_files


def create_zip_archive(data_files):
    """Create a ZIP archive from the data files"""
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, file_data in data_files.items():
            zip_file.writestr(file_name, file_data)

    zip_buffer.seek(0)
    return zip_buffer.getvalue()


def delete_user_data(connection, cursor, user_id):
    """Delete or anonymize user data"""
    try:
        # Start with creating a backup of the data before deletion
        # (This is a good practice for compliance and potential recovery)
        data_files = export_user_data(cursor, user_id)
        zip_data = create_zip_archive(data_files)

        # Upload backup to S3
        backup_key = f"deletion_backups/{user_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
        s3_client.put_object(
            Bucket=DATA_EXPORTS_BUCKET,
            Key=backup_key,
            Body=zip_data,
            Metadata={
                'user_id': str(user_id),
                'deletion_date': datetime.now().isoformat()
            }
        )

        # Step 1: Anonymize user details in users table
        cursor.execute("""
            UPDATE users
            SET email = %s, password = %s, salt = NULL, isdisabled = TRUE, 
                stripecustomerid = NULL, updatedat = NOW()
            WHERE userid = %s
        """, (f"deleted_user_{user_id}@example.com", "DELETED_USER_DATA", user_id))

        # Step 2: Delete sensitive user details
        cursor.execute("""
            DELETE FROM userdetails
            WHERE userid = %s
        """, (user_id,))

        # Step 3: Delete payment methods
        cursor.execute("""
            DELETE FROM userpaymentsources
            WHERE userid = %s
        """, (user_id,))

        # Step 4: Delete user preferences
        cursor.execute("""
            DELETE FROM user_preferences
            WHERE userid = %s
        """, (user_id,))

        # Step 5: Anonymize but don't delete order history (for business records)
        cursor.execute("""
            UPDATE orderdetails
            SET notes = 'Data deleted per user request'
            WHERE orderid IN (SELECT orderid FROM orders WHERE userid = %s)
        """, (user_id,))

        # Step 6: Anonymize user review comments
        cursor.execute("""
            UPDATE reviews
            SET comments = 'Content removed per user request'
            WHERE userid = %s
        """, (user_id,))

        # Step 7: Anonymize user activity logs
        cursor.execute("""
            UPDATE user_activity_logs
            SET details = '{"message": "Data anonymized per user request"}'
            WHERE userid = %s
        """, (user_id,))

        # Step The deletion request record
        cursor.execute("""
            UPDATE data_requests
            SET status = 'COMPLETED', completedat = NOW(), updatedat = NOW()
            WHERE userid = %s AND requesttype = 'delete' AND status = 'PENDING'
        """, (user_id,))

        # Log the successful deletion
        cursor.execute("""
            INSERT INTO user_activity_logs (userid, activity_type, details, createdat)
            VALUES (%s, %s, %s, NOW())
        """, (
            user_id,
            'DATA_DELETION_COMPLETED',
            json.dumps({
                'message': 'User data deleted/anonymized as requested',
                'backup_key': backup_key,
                'timestamp': datetime.now().isoformat()
            })
        ))

        connection.commit()

        return True, "User data successfully deleted/anonymized"

    except Exception as e:
        connection.rollback()
        logger.error(f"Failed to delete user data: {e}")
        return False, str(e)


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Process SNS records
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            # Extract data from message
            request_id = message.get('request_id')
            user_id = message.get('user_id')
            email = message.get('email')
            name = message.get('name')
            request_type = message.get('request_type')

            # Connect to database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Verify request exists and is pending
            cursor.execute("""
                SELECT requestid, status
                FROM data_requests
                WHERE requestid = %s
            """, (request_id,))

            request_info = cursor.fetchone()

            if not request_info:
                logger.error(f"Request {request_id} not found")
                continue

            if request_info['status'] != 'PENDING':
                logger.warning(f"Request {request_id} is not in PENDING status")
                continue

            # Process based on request type
            if request_type == 'export':
                try:
                    # Export user data
                    data_files = export_user_data(cursor, user_id)

                    # Create a ZIP archive
                    zip_data = create_zip_archive(data_files)

                    # Generate a timestamp for the filename
                    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                    filename = f"user_data_export_{user_id}_{timestamp}.zip"

                    # Upload to S3
                    s3_key = f"exports/{user_id}/{filename}"
                    s3_client.put_object(
                        Bucket=DATA_EXPORTS_BUCKET,
                        Key=s3_key,
                        Body=zip_data,
                        Metadata={
                            'user_id': str(user_id),
                            'export_date': datetime.now().isoformat()
                        }
                    )

                    # Generate a pre-signed URL (valid for 24 hours)
                    presigned_url = s3_client.generate_presigned_url(
                        'get_object',
                        Params={
                            'Bucket': DATA_EXPORTS_BUCKET,
                            'Key': s3_key
                        },
                        ExpiresIn=86400  # 24 hours
                    )

                    # Update request status
                    cursor.execute("""
                        UPDATE data_requests
                        SET status = 'COMPLETED', exporturl = %s, completedat = NOW(), updatedat = NOW()
                        WHERE requestid = %s
                    """, (presigned_url, request_id))

                    # Log the successful export
                    cursor.execute("""
                        INSERT INTO user_activity_logs (userid, activity_type, details, createdat)
                        VALUES (%s, %s, %s, NOW())
                    """, (
                        user_id,
                        'DATA_EXPORT_COMPLETED',
                        json.dumps({
                            'request_id': request_id,
                            's3_key': s3_key,
                            'download_url': presigned_url,
                            'expiry_time': (datetime.now() + timedelta(days=1)).isoformat()
                        })
                    ))

                    connection.commit()

                    # Send email notification with download link
                    if email:
                        email_subject = "Your Data Export is Ready"
                        email_message = f"""
                        <h2>Your Data Export is Ready</h2>
                        <p>Dear {name},</p>
                        <p>We have processed your request to export your personal data. You can download your data using the link below:</p>
                        <div style="margin: 20px; padding: 15px; background-color: #f0f0f0; border-radius: 5px; text-align: center;">
                            <a href="{presigned_url}" style="background-color: #4CAF50; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; font-weight: bold;">
                                Download Your Data
                            </a>
                        </div>
                        <p><strong>Important:</strong> This download link will expire in 24 hours. Please download your data before the link expires.</p>
                        <p>If you have any questions about your data export, please contact our support team.</p>
                        <p>Thank you for using our service!</p>
                        """

                        try:
                            send_email_via_ses(email, email_subject, email_message)
                            logger.info(f"Data export notification email sent to user {user_id}")
                        except Exception as e:
                            logger.error(f"Failed to send data export notification email: {e}")

                    # Log success to SNS
                    log_to_sns(1, 7, 9, 1, {
                        "user_id": user_id,
                        "request_id": request_id,
                        "s3_key": s3_key
                    }, "Data Export - Success", user_id)

                    logger.info(f"Successfully exported data for user {user_id}")

                except Exception as export_error:
                    logger.error(f"Failed to export data: {export_error}")

                    # Update request status to failed
                    cursor.execute("""
                        UPDATE data_requests
                        SET status = 'FAILED', notes = %s, updatedat = NOW()
                        WHERE requestid = %s
                    """, (str(export_error), request_id))

                    connection.commit()

                    # Log error to SNS
                    log_to_sns(4, 7, 9, 43, {
                        "user_id": user_id,
                        "request_id": request_id,
                        "error": str(export_error)
                    }, "Data Export - Failed", user_id)

            elif request_type == 'delete':
                try:
                    # Delete user data
                    success, message = delete_user_data(connection, cursor, user_id)

                    if success:
                        # Send email notification
                        if email:
                            email_subject = "Data Deletion Confirmation"
                            email_message = f"""
                            <h2>Data Deletion Confirmation</h2>
                            <p>Dear {name},</p>
                            <p>We have completed your request to delete your personal data from our systems. Your account has been anonymized and your personal information has been removed as requested.</p>
                            <p>Please note that some information may be retained for legal and business purposes as outlined in our Privacy Policy.</p>
                            <p>If you have any questions about this data deletion, please contact our support team.</p>
                            <p>Thank you for having been a valued user of our service.</p>
                            """

                            try:
                                send_email_via_ses(email, email_subject, email_message)
                                logger.info(f"Data deletion confirmation email sent to user {user_id}")
                            except Exception as e:
                                logger.error(f"Failed to send data deletion confirmation email: {e}")

                        # Log success to SNS
                        log_to_sns(1, 7, 9, 1, {
                            "user_id": user_id,
                            "request_id": request_id
                        }, "Data Deletion - Success", user_id)

                        logger.info(f"Successfully deleted data for user {user_id}")
                    else:
                        # Update request status to failed
                        cursor.execute("""
                            UPDATE data_requests
                            SET status = 'FAILED', notes = %s, updatedat = NOW()
                            WHERE requestid = %s
                        """, (message, request_id))

                        connection.commit()

                        # Log error to SNS
                        log_to_sns(4, 7, 9, 43, {
                            "user_id": user_id,
                            "request_id": request_id,
                            "error": message
                        }, "Data Deletion - Failed", user_id)

                except Exception as delete_error:
                    logger.error(f"Failed to delete data: {delete_error}")

                    # Update request status to failed
                    cursor.execute("""
                        UPDATE data_requests
                        SET status = 'FAILED', notes = %s, updatedat = NOW()
                        WHERE requestid = %s
                    """, (str(delete_error), request_id))

                    connection.commit()

                    # Log error to SNS
                    log_to_sns(4, 7, 9, 43, {
                        "user_id": user_id,
                        "request_id": request_id,
                        "error": str(delete_error)
                    }, "Data Deletion - Failed", user_id)

            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Data {request_type} request processed successfully',
                    'request_id': request_id,
                    'user_id': user_id,
                    'status': 'COMPLETED'
                })
            }

    except Exception as e:
        logger.error(f"Failed to process data management request: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 7, 9, 43, {
            "request_id": request_id if 'request_id' in locals() else 'unknown',
            "error": str(e)
        }, "Data Management - Failed", user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process data management request',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()