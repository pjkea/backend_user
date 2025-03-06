import json
import logging
import boto3
from decimal import Decimal
from serviceRequest.layers.utils import get_secrets

# Initialize AWS services
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load secrets
secrets = get_secrets()

# SNS topics
SERVICE_REQUEST_TOPIC_ARN = secrets["SERVICE_REQUEST_TOPIC_ARN"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))

        # Extract selections from request
        userid = body.get("user")
        base_package = body.get('package', {})
        add_ons = body.get('addOns', [])
        promo_code = body.get('promoCode')
        quantity = body.get('quantity', 1)

        # Initialize price calculation
        base_price = Decimal(str(base_package.get('price', 0)))
        add_on_price = sum(Decimal(str(add_on.get('price', 0))) for add_on in add_ons)

        # Apply quantity multiplier
        subtotal = (base_price + add_on_price) * Decimal(str(quantity))

        # Calculate tax (if applicable)
        tax_rate = Decimal('0.0725')  # Example: 7.25% tax rate
        taxable_amount = subtotal
        tax = taxable_amount * tax_rate if taxable_amount > 0 else 0

        # Calculate final price
        total_price = taxable_amount + tax

        price_detail = {
            'basePrice': float(base_price),
            'addOnPrice': float(add_on_price),
            'quantity': quantity,
            'subtotal': float(subtotal),
            'tax': float(tax),
            'totalPrice': float(total_price),
            'currency': 'USD'
        }

        # Send price to SNS
        sns_client.publish(
            TopicArn=SERVICE_REQUEST_TOPIC_ARN,
            Message=json.dumps({
                "user_id": userid,
                "package": base_package,
                "add_ons": add_ons,
                "price": price_detail
            }),
            Subject='Requested Service Price',
        )
        logger.info(json.dumps(price_detail))

        # Log success
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 1,
                "categoryid": 1,  # Service Request
                "transactiontypeid": 12,  # Address Update
                "statusid": 27, # Sucess
                "user_id": userid,
            })
        )
        logger.info('Successfully calculated service price')

        return {
            "statusCode": 200,
            'body': json.dumps({'priceDetail': price_detail})
        }

    except Exception as e:
        logger.error(f'Failed to calculate price: {str(e)}')

        # Lod error
        sns_client.publish(
            TopicArn=SNS_LOGGING_TOPIC_ARN,
            Message=json.dumps({
                "logtypeid": 4,
                "categoryid": 1,  # Service Request
                "transactiontypeid": 12,  # Address Update(ignore)
                "statusid": 43, # Failure
                "user_id": userid,
                "package": base_package,
                "add_ons": add_ons,
                "price": price_detail
            })
        )

        return {
            "statusCode": 500,
            "body": json.dumps({'error': str(e)})
        }











