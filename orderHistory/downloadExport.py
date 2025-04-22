import json
import logging
from datetime import datetime
from typing import Dict, Any
from layers.utils import get_db_connection, log_to_sns

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function to handle export downloads.
    Retrieves the export data from the database and returns it with appropriate headers.
    """
    try:
        # Get request_id from path parameters
        request_id = event.get('pathParameters', {}).get('request_id')
        if not request_id:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing request_id parameter'
                })
            }
        
        # Get database connection
        conn = get_db_connection()
        cur = conn.cursor()
        
        try:
            # Query the export
            query = """
                SELECT 
                    format,
                    file_data,
                    file_size,
                    created_at,
                    expires_at
                FROM exports
                WHERE request_id = %s
                AND expires_at > NOW()
            """
            
            cur.execute(query, (request_id,))
            export = cur.fetchone()
            
            if not export:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'error': 'Export not found or expired'
                    })
                }
            
            format_type, file_data, file_size, created_at, expires_at = export
            
            # Set content type based on format
            content_types = {
                'csv': 'text/csv',
                'excel': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'pdf': 'application/pdf'
            }
            
            content_type = content_types.get(format_type, 'application/octet-stream')
            
            # Set filename
            filename = f"order_history_{request_id}.{format_type}"
            
            # Log successful download
            log_to_sns(
                logtypeid=2,
                categoryid=1,
                transactiontypeid=2,
                statusid=1,  # Success status
                message=f"Export downloaded: {request_id}",
                subject="Export Download",
                userid=None  # We don't have user_id in this context
            )
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': content_type,
                    'Content-Disposition': f'attachment; filename="{filename}"',
                    'Content-Length': str(file_size),
                    'Access-Control-Allow-Origin': '*'
                },
                'body': file_data.decode('utf-8') if format_type == 'csv' else file_data,
                'isBase64Encoded': format_type != 'csv'
            }
            
        finally:
            cur.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Error downloading export: {str(e)}", exc_info=True)
        
        # Log the error
        log_to_sns(
            logtypeid=2,
            categoryid=1,
            transactiontypeid=2,
            statusid=2,  # Error status
            message=f"Error downloading export: {str(e)}",
            subject="Export Download Error",
            userid=None
        )
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        } 