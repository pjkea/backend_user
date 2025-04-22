import json
import logging
import csv
import pandas as pd
from datetime import datetime
from typing import Dict, Any
import os
from layers.utils import get_db_connection, log_to_sns, get_secrets

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def generate_csv(orders: list) -> str:
    """Generate CSV data from orders."""
    if not orders:
        return None
        
    # Convert orders to DataFrame
    df = pd.DataFrame(orders)
    
    # Generate CSV data
    return df.to_csv(index=False)

def generate_excel(orders: list) -> bytes:
    """Generate Excel data from orders."""
    if not orders:
        return None
        
    # Convert orders to DataFrame
    df = pd.DataFrame(orders)
    
    # Generate Excel data
    return df.to_excel(index=False, engine='openpyxl')

def generate_pdf(orders: list) -> bytes:
    """Generate PDF data from orders."""
    if not orders:
        return None
        
    # Convert orders to DataFrame
    df = pd.DataFrame(orders)
    
    # Generate PDF data
    # Note: You'll need to add a PDF generation library to your Lambda layer
    # For example: pdfkit, weasyprint, or reportlab
    return df.to_html(index=False).encode('utf-8')

def store_export(conn, cur, request_id: str, user_id: str, export_format: str, 
                 file_data: bytes, file_size: int) -> str:
    """Store export data in the database."""
    try:
        # Insert into exports table
        query = """
            INSERT INTO exports (
                request_id, user_id, format, file_data, file_size,
                created_at, expires_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING request_id
        """
        
        created_at = datetime.now()
        expires_at = created_at + pd.Timedelta(days=7)  # Exports expire after 7 days
        
        cur.execute(query, (
            request_id,
            user_id,
            export_format,
            file_data,
            file_size,
            created_at,
            expires_at
        ))
        
        conn.commit()
        return request_id
        
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to store export: {str(e)}")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function to process order history export requests.
    Generates the requested file format and stores in PostgreSQL.
    """
    try:
        # Parse SNS message
        sns_message = json.loads(event['Records'][0]['Sns']['Message'])
        request_id = sns_message['request_id']
        user_id = sns_message['userid']
        export_format = sns_message['format']
        start_date = sns_message['start_date']
        end_date = sns_message['end_date']
        service_type = sns_message.get('service_type')
        
        # Get database connection
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            # Query orders
            query = """
                SELECT 
                    o.order_id,
                    o.service_type,
                    o.status,
                    o.created_at,
                    o.updated_at,
                    o.address_id,
                    o.total_amount,
                    o.payment_status,
                    a.street_address,
                    a.city,
                    a.state,
                    a.postal_code
                FROM orders o
                LEFT JOIN addresses a ON o.address_id = a.address_id
                WHERE o.user_id = %s 
                AND o.created_at BETWEEN %s AND %s
            """
            
            params = [user_id, start_date, end_date]
            if service_type:
                query += " AND o.service_type = %s"
                params.append(service_type)
                
            query += " ORDER BY o.created_at DESC"
            
            cur.execute(query, params)
            orders = cur.fetchall()
            
            # Convert orders to list of dictionaries
            order_list = []
            for order in orders:
                order_dict = {
                    'order_id': order[0],
                    'service_type': order[1],
                    'status': order[2],
                    'created_at': order[3].isoformat() if order[3] else None,
                    'updated_at': order[4].isoformat() if order[4] else None,
                    'address_id': order[5],
                    'total_amount': float(order[6]) if order[6] else None,
                    'payment_status': order[7],
                    'address': {
                        'street_address': order[8],
                        'city': order[9],
                        'state': order[10],
                        'postal_code': order[11]
                    } if all([order[8], order[9], order[10], order[11]]) else None
                }
                order_list.append(order_dict)
            
            # Generate file data based on format
            file_data = None
            if export_format == 'csv':
                file_data = generate_csv(order_list).encode('utf-8')
            elif export_format == 'excel':
                file_data = generate_excel(order_list)
            elif export_format == 'pdf':
                file_data = generate_pdf(order_list)
            
            if not file_data:
                raise Exception(f"Failed to generate {export_format} file")
            
            # Store export in database
            file_size = len(file_data)
            stored_request_id = store_export(
                conn, cur, request_id, user_id, export_format, file_data, file_size
            )
            
            # Log successful export
            log_to_sns(
                logtypeid=2,
                categoryid=1,
                transactiontypeid=2,
                statusid=1,  # Success status
                message=f"Order history export completed for user {user_id}",
                subject="Order History Export Completed",
                userid=user_id
            )
            
            # Get the API endpoint from secrets
            secrets = get_secrets()
            api_endpoint = secrets.get('API_ENDPOINT')
            
            # Generate download URL
            download_url = f"{api_endpoint}/exports/{stored_request_id}/download"
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Export completed successfully',
                    'request_id': stored_request_id,
                    'download_url': download_url,
                    'expires_in': 7  # days
                })
            }
            
        finally:
            cur.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error processing export: {str(e)}", exc_info=True)
        
        # Log the error
        log_to_sns(
            logtypeid=2,
            categoryid=1,
            transactiontypeid=2,
            statusid=2,  # Error status
            message=f"Error processing export: {str(e)}",
            subject="Order History Export Error",
            userid=user_id if 'user_id' in locals() else None
        )
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        } 