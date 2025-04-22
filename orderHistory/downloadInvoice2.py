import json
import boto3
import logging
import io
import uuid
from datetime import datetime
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

# S3 bucket for storing documents
DOCUMENT_BUCKET = secrets["DOCUMENT_BUCKET"]


def generate_invoice_html(order_details):
    """Generate HTML content for an invoice"""
    order_id = order_details.get('orderid', 'Unknown')
    customer_name = f"{order_details.get('customer_firstname', '')} {order_details.get('customer_lastname', '')}"
    customer_email = order_details.get('customer_email', '')
    tidysp_name = f"{order_details.get('tidysp_firstname', '')} {order_details.get('tidysp_lastname', '')}"
    service_name = order_details.get('productname', 'Service')
    service_date = order_details.get('completedat', order_details.get('createdat', ''))
    address = order_details.get('address', '')

    # Format price details
    base_price = float(order_details.get('base_price', 0))
    total_price = float(order_details.get('totalprice', 0))

    # Calculate add-ons price
    add_ons_price = 0
    add_ons_html = ""

    if order_details.get('add_ons'):
        if isinstance(order_details['add_ons'], list):
            add_ons = order_details['add_ons']
        elif isinstance(order_details['add_ons'], str):
            try:
                add_ons = json.loads(order_details['add_ons'])
            except:
                add_ons = []
        else:
            add_ons = []

        for addon in add_ons:
            price = float(addon.get('price', 0))
            add_ons_price += price
            add_ons_html += f"""
            <tr>
                <td>{addon.get('name', 'Add-on')}</td>
                <td>${price:.2f}</td>
            </tr>
            """

    # Calculate tax
    subtotal = base_price + add_ons_price
    tax = total_price - subtotal if total_price > subtotal else 0

    # Generate invoice HTML
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Invoice #{order_id}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
            .invoice-container {{ max-width: 800px; margin: 0 auto; padding: 30px; border: 1px solid #eee; box-shadow: 0 0 10px rgba(0, 0, 0, 0.15); }}
            .invoice-header {{ display: flex; justify-content: space-between; margin-bottom: 20px; padding-bottom: 20px; border-bottom: 1px solid #eee; }}
            .invoice-details {{ margin-bottom: 40px; }}
            table {{ width: 100%; border-collapse: collapse; }}
            th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #eee; }}
            .amount-table td:last-child {{ text-align: right; }}
            .total {{ font-weight: bold; font-size: 1.2em; }}
            .footer {{ margin-top: 30px; text-align: center; color: #777; font-size: 0.9em; }}
        </style>
    </head>
    <body>
        <div class="invoice-container">
            <div class="invoice-header">
                <div>
                    <h1>INVOICE</h1>
                    <p>TidyZon Services</p>
                </div>
                <div>
                    <h2>Invoice #{order_id}</h2>
                    <p>Date: {datetime.now().strftime('%B %d, %Y')}</p>
                </div>
            </div>

            <div class="invoice-details">
                <div style="display: flex; justify-content: space-between;">
                    <div>
                        <h3>Bill To:</h3>
                        <p>{customer_name}<br>
                        {customer_email}<br>
                        {address}</p>
                    </div>
                    <div>
                        <h3>Service Provider:</h3>
                        <p>{tidysp_name}</p>
                    </div>
                </div>

                <div style="margin-top: 20px;">
                    <h3>Service Details:</h3>
                    <p>Service: {service_name}<br>
                    Date of Service: {service_date if isinstance(service_date, str) else service_date.strftime('%B %d, %Y')}<br>
                    Location: {address}</p>
                </div>
            </div>

            <table class="amount-table">
                <tr>
                    <th>Item</th>
                    <th>Amount</th>
                </tr>
                <tr>
                    <td>{service_name} (Base Service)</td>
                    <td>${base_price:.2f}</td>
                </tr>
                {add_ons_html}
                <tr>
                    <td>Subtotal</td>
                    <td>${subtotal:.2f}</td>
                </tr>
                <tr>
                    <td>Tax</td>
                    <td>${tax:.2f}</td>
                </tr>
                <tr class="total">
                    <td>Total</td>
                    <td>${total_price:.2f}</td>
                </tr>
            </table>

            <div class="footer">
                <p>Thank you for choosing TidyZon Services.</p>
                <p>For questions about this invoice, please contact support@tidyzon.com</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html


def generate_receipt_html(order_details):
    """Generate HTML content for a receipt"""
    # Similar to invoice but with payment information
    payment_info = {
        'payment_id': order_details.get('paymentid', 'Unknown'),
        'transaction_id': order_details.get('transactionid', 'Unknown'),
        'payment_method': order_details.get('paymentgateway', 'Credit Card'),
        'payment_status': order_details.get('payment_status', 'COMPLETED'),
        'payment_date': order_details.get('paymentdate', datetime.now().strftime('%B %d, %Y'))
    }

    # Start with the invoice HTML as a base
    receipt_html = generate_invoice_html(order_details)

    # Add payment information section
    payment_info_html = f"""
    <div style="margin-top: 30px; border-top: 1px solid #eee; padding-top: 20px;">
        <h3>Payment Information:</h3>
        <table>
            <tr>
                <td>Payment ID:</td>
                <td>{payment_info['payment_id']}</td>
            </tr>
            <tr>
                <td>Transaction ID:</td>
                <td>{payment_info['transaction_id']}</td>
            </tr>
            <tr>
                <td>Payment Method:</td>
                <td>{payment_info['payment_method']}</td>
            </tr>
            <tr>
                <td>Status:</td>
                <td>{payment_info['payment_status']}</td>
            </tr>
            <tr>
                <td>Date:</td>
                <td>{payment_info['payment_date']}</td>
            </tr>
        </table>
    </div>
    """

    # Insert payment info before the footer
    receipt_html = receipt_html.replace('<div class="footer">', payment_info_html + '<div class="footer">')

    return receipt_html


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Process SNS records
        for record in event['Records']:
            message = json.loads(record['Sns']['Message'])

            # Extract data from message
            document_id = message.get('document_id')
            order_id = message.get('order_id')
            user_id = message.get('user_id')
            document_type = message.get('document_type', 'INVOICE')
            order_details = message.get('order_details', {})

            # Connect to database
            connection = get_db_connection()
            cursor = connection.cursor(cursor_factory=RealDictCursor)

            # Get user contact information
            cursor.execute("""
                SELECT u.email, ud.phonenumber, ud.firstname, ud.lastname
                FROM users u
                JOIN userdetails ud ON u.userid = ud.userid
                WHERE u.userid = %s
            """, (user_id,))

            user_info = cursor.fetchone()

            if not user_info:
                logger.warning(f"User information not found for user {user_id}")
                continue

            user_email = user_info['email']
            user_name = f"{user_info['firstname']} {user_info['lastname']}"

            # Generate HTML content based on document type
            if document_type == 'INVOICE':
                html_content = generate_invoice_html(order_details)
                filename = f"invoice_{order_id}_{datetime.now().strftime('%Y%m%d')}.html"
            else:  # RECEIPT
                html_content = generate_receipt_html(order_details)
                filename = f"receipt_{order_id}_{datetime.now().strftime('%Y%m%d')}.html"

            # Upload to S3
            s3_key = f"documents/{user_id}/{filename}"
            s3_client.put_object(
                Bucket=DOCUMENT_BUCKET,
                Key=s3_key,
                Body=html_content.encode('utf-8'),
                ContentType='text/html',
                Metadata={
                    'user_id': str(user_id),
                    'order_id': str(order_id),
                    'document_type': document_type
                }
            )

            # Generate a pre-signed URL for downloading the document
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': DOCUMENT_BUCKET,
                    'Key': s3_key
                },
                ExpiresIn=604800  # 7 days in seconds
            )

            # Update the document record with the URL and completed status
            cursor.execute("""
                UPDATE documents
                SET documenturl = %s, status = 'COMPLETED', updatedat = NOW()
                WHERE documentid = %s
            """, (presigned_url, document_id))

            # Create notification in app
            cursor.execute("""
                INSERT INTO notifications (userid, notificationtype, message, isread, createdat)
                VALUES (%s, %s, %s, %s, NOW())
            """, (
            user_id, 'DOCUMENT', f"Your {document_type.lower()} for order #{order_id} is now available for download.",
            False))

            # Send email notification
            if user_email:
                email_subject = f"Your {document_type} for Order #{order_id} is Ready"
                email_message = f"""
                <h2>{document_type} Ready for Download</h2>
                <p>Dear {user_name},</p>
                <p>Your {document_type.lower()} for order #{order_id} is now ready for download.</p>
                <p>You can download it by clicking the button below or by visiting your order history in the app.</p>
                <div style="text-align: center; margin: 30px 0;">
                    <a href="{presigned_url}" 
                       style="background-color: #4CAF50; color: white; padding: 12px 20px; text-decoration: none; border-radius: 4px; font-weight: bold;">
                       Download {document_type}
                    </a>
                </div>
                <p>Note: This download link will expire in 7 days.</p>
                <p>Thank you for choosing TidyZon Services.</p>
                """

                try:
                    send_email_via_ses(user_email, email_subject, email_message)
                    logger.info(f"Document notification email sent to user {user_id}")
                except Exception as e:
                    logger.error(f"Failed to send email: {e}")

            connection.commit()

            # Log success to SNS
            log_to_sns(1, 11, 21, 1, {
                "user_id": user_id,
                "order_id": order_id,
                "document_id": document_id,
                "document_type": document_type,
                "document_url": presigned_url
            }, f"Generate {document_type} - Success", user_id)

            logger.info(f"Successfully generated {document_type} for order {order_id}")

            # Return success response
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'{document_type} generated successfully',
                    'document_id': document_id,
                    'document_url': presigned_url,
                    'status': 'COMPLETED'
                })
            }

    except Exception as e:
        logger.error(f"Failed to generate document: {e}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error to SNS
        log_to_sns(4, 11, 21, 43, {
            "document_id": document_id if 'document_id' in locals() else 'unknown',
            "error": str(e)
        }, f"Generate {document_type if 'document_type' in locals() else 'Document'} - Failed",
                   user_id if 'user_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to generate document',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()