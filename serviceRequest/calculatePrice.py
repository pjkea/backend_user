import json
import logging
import boto3
from decimal import Decimal

from psycopg2.extras import RealDictCursor

from serviceRequest.layers.utils import get_secrets, get_db_connection, log_to_sns

# Initialize AWS services
secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")
sns_client = boto3.client("sns", region_name="us-east-1")

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load secrets
secrets = get_secrets()

# SNS topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))

        # Extract selections from request
        userid = body.get("user")
        product_id = body.get('productid', {})
        addons = body.get('addonsid', [])
        vehicle_type = body.get('vehicletype', 'Sedan')
        promo_code = body.get('promoCode')
        quantity = body.get('quantity', 1)

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
                        SELECT price FROM service_vehicle_pricing 
                        WHERE productid = %s AND vehicletype = %s
                    """, (product_id, vehicle_type))
        vehicle_result = cursor.fetchone()

        if vehicle_result:
            vehicle_price = vehicle_result['price']

        cursor.execute("""
                    SELECT productname, price, duration FROM serviceproducts 
                    WHERE productid = %s
                """, (product_id,))
        product_result = cursor.fetchone()

        if not product_result:
            raise Exception(f"Product with ID {product_id} not found")

        addon_details = []
        addon_price_total = Decimal('0')

        if addons:
            placeholders = ','.join(['%s'] * len(addons))
            cursor.execute(f"""
                SELECT priceid, addonname, addonprice, addonduration FROM service_pricing 
                WHERE priceid IN ({placeholders}) AND productid = %s
            """, addons + [product_id])

            addon_results = cursor.fetchall()

            for addon in addon_results:
                addon_details.append({
                    "id": addon['priceid'],
                    "name": addon['addonname'],
                    "price": float(addon['addonprice']),
                    "duration": addon['addonduration']
                })
                addon_price_total += Decimal(str(addon['addonprice']))

        conn.commit()

        # Initialize price calculation
        base_price = Decimal(str(product_result['price']))

        # Apply quantity multiplier
        subtotal = (base_price + addon_price_total) * Decimal(str(quantity))

        # Calculate tax (if applicable)
        tax_rate = Decimal('0.0725')  # Example: 7.25% tax rate
        taxable_amount = subtotal
        tax = taxable_amount * tax_rate if taxable_amount > 0 else 0

        # Calculate final price
        total_price = taxable_amount + tax

        package_info = {
            'id': product_result['productid'],
            'name': product_result['productname'],
            'price': float(base_price),
            'duration': product_result['duration'],
            'vehicleType': vehicle_type
        }

        price_detail = {
            'basePrice': float(base_price),
            'addOnPrice': float(addon_price_total),
            'quantity': quantity,
            'subtotal': float(subtotal),
            'tax': float(tax),
            'totalPrice': float(total_price),
            'currency': 'USD'
        }

        calculation_details = {
            'package': package_info,
            'addOns': addon_details,
            'priceDetail': price_detail
        }

        # Log success
        log_to_sns(1,1,12,27,{price_detail}, '', userid)

        logger.info('Successfully calculated service price')

        return {
            "statusCode": 200,
            'body': json.dumps({calculation_details})
        }

    except Exception as e:
        logger.error(f'Failed to calculate price: {str(e)}')

        # Lod error
        log_to_sns(4,1,12,43,{price_detail}, '', userid)

        return {
            "statusCode": 500,
            "body": json.dumps({'error': str(e)})
        }

    finally:
        cursor.close()
        conn.close()











