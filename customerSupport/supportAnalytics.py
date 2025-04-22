import json
import boto3
import logging
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

from customerSupport.layers.utils import get_secrets, get_db_connection, log_to_sns

# Initialize AWS services
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')

# Load secrets
secrets = get_secrets()

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# SNS Topics
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]


def process_resolution_analytics(message, cursor):
    """Process analytics for ticket resolution events"""
    try:
        ticket_id = message.get('ticket_id')
        department_id = message.get('department_id')
        satisfaction_rating = message.get('satisfaction_rating')
        is_resolved = message.get('is_resolved', True)

        if not ticket_id or department_id is None:
            raise ValueError("Missing required analytics data")

        # Get ticket creation time to calculate resolution time
        cursor.execute("""
            SELECT created_at FROM support_tickets
            WHERE ticket_id = %s
        """, (ticket_id,))

        ticket = cursor.fetchone()

        if ticket:
            logger.info(f"Ticket details retrieved for analytics, ticket ID {ticket_id}")

            creation_time = ticket['created_at']
            resolution_time = datetime.now()

            # Calculate time to resolution in hours
            time_to_resolution = (resolution_time - creation_time).total_seconds() / 3600

            # Insert into analytics table
            cursor.execute("""
                INSERT INTO support_analytics
                (ticket_id, department_id, event_type, satisfaction_rating, is_resolved, 
                time_to_resolution_hours, event_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING analytics_id
            """, (
                ticket_id,
                department_id,
                'resolution',
                satisfaction_rating,
                is_resolved,
                time_to_resolution,
                datetime.now()
            ))

            result = cursor.fetchone()

            if result:
                logger.info(f"Analytics record created for ticket resolution, ID {result['analytics_id']}")

                # Update department metrics if resolved with rating
                if is_resolved and satisfaction_rating is not None:
                    # Get current department metrics
                    cursor.execute("""
                        SELECT total_ratings, sum_ratings, avg_satisfaction 
                        FROM department_performance
                        WHERE department_id = %s
                    """, (department_id,))

                    dept_metrics = cursor.fetchone()

                    if dept_metrics:
                        logger.info(f"Department metrics retrieved for department ID {department_id}")

                        # Calculate new average
                        total_ratings = dept_metrics['total_ratings'] + 1
                        sum_ratings = dept_metrics['sum_ratings'] + satisfaction_rating
                        avg_satisfaction = sum_ratings / total_ratings

                        # Update department metrics
                        cursor.execute("""
                            UPDATE department_performance
                            SET total_ratings = %s, sum_ratings = %s, avg_satisfaction = %s,
                                last_updated = %s
                            WHERE department_id = %s
                        """, (
                            total_ratings,
                            sum_ratings,
                            avg_satisfaction,
                            datetime.now(),
                            department_id
                        ))
                    else:
                        logger.info(f"Creating new department metrics for department ID {department_id}")

                        # Create new department metrics record
                        cursor.execute("""
                            INSERT INTO department_performance
                            (department_id, total_ratings, sum_ratings, avg_satisfaction, last_updated)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            department_id,
                            1,
                            satisfaction_rating,
                            satisfaction_rating,
                            datetime.now()
                        ))
            else:
                logger.warning(f"Failed to create analytics record for ticket resolution, ticket ID {ticket_id}")
        else:
            logger.warning(f"No ticket details found for analytics, ticket ID {ticket_id}")

        return True

    except Exception as e:
        logger.error(f"Error processing resolution analytics: {str(e)}")
        return False


def process_ticket_creation_analytics(message, cursor):
    """Process analytics for ticket creation events"""
    try:
        ticket_id = message.get('ticket_id')
        department_id = message.get('department_id')
        category_id = message.get('category_id')

        if not ticket_id or department_id is None:
            raise ValueError("Missing required analytics data")

        # Insert into analytics table
        cursor.execute("""
            INSERT INTO support_analytics
            (ticket_id, department_id, category_id, event_type, event_timestamp)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING analytics_id
        """, (
            ticket_id,
            department_id,
            category_id,
            'creation',
            datetime.now()
        ))

        result = cursor.fetchone()

        if result:
            logger.info(f"Analytics record created for ticket creation, ID {result['analytics_id']}")

            # Update ticket volume metrics
            current_date = datetime.now().date()

            cursor.execute("""
                SELECT ticket_count FROM ticket_volume
                WHERE department_id = %s AND date = %s
            """, (department_id, current_date))

            volume_record = cursor.fetchone()

            if volume_record:
                logger.info(f"Volume record found for department ID {department_id} on {current_date}")

                # Update existing record
                cursor.execute("""
                    UPDATE ticket_volume
                    SET ticket_count = ticket_count + 1
                    WHERE department_id = %s AND date = %s
                """, (department_id, current_date))
            else:
                logger.info(f"Creating new volume record for department ID {department_id} on {current_date}")

                # Create new record
                cursor.execute("""
                    INSERT INTO ticket_volume
                    (department_id, date, ticket_count)
                    VALUES (%s, %s, %s)
                """, (department_id, current_date, 1))
        else:
            logger.warning(f"Failed to create analytics record for ticket creation, ticket ID {ticket_id}")

        return True

    except Exception as e:
        logger.error(f"Error processing creation analytics: {str(e)}")
        return False


def process_agent_performance_analytics(message, cursor):
    """Process analytics for agent response events"""
    try:
        ticket_id = message.get('ticket_id')
        agent_id = message.get('agent_id')
        response_time_minutes = message.get('response_time_minutes')

        if not all([ticket_id, agent_id, response_time_minutes is not None]):
            raise ValueError("Missing required agent performance data")

        # Insert into analytics table
        cursor.execute("""
            INSERT INTO support_analytics
            (ticket_id, agent_id, event_type, response_time_minutes, event_timestamp)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING analytics_id
        """, (
            ticket_id,
            agent_id,
            'agent_response',
            response_time_minutes,
            datetime.now()
        ))

        result = cursor.fetchone()

        if result:
            logger.info(f"Analytics record created for agent response, ID {result['analytics_id']}")

            # Update agent performance metrics
            cursor.execute("""
                SELECT total_responses, sum_response_time, avg_response_time 
                FROM agent_performance
                WHERE agent_id = %s
            """, (agent_id,))

            agent_metrics = cursor.fetchone()

            if agent_metrics:
                logger.info(f"Agent metrics retrieved for agent ID {agent_id}")

                # Calculate new average
                total_responses = agent_metrics['total_responses'] + 1
                sum_response_time = agent_metrics['sum_response_time'] + response_time_minutes
                avg_response_time = sum_response_time / total_responses

                # Update agent metrics
                cursor.execute("""
                    UPDATE agent_performance
                    SET total_responses = %s, sum_response_time = %s, 
                        avg_response_time = %s, last_updated = %s
                    WHERE agent_id = %s
                """, (
                    total_responses,
                    sum_response_time,
                    avg_response_time,
                    datetime.now(),
                    agent_id
                ))
            else:
                logger.info(f"Creating new agent metrics for agent ID {agent_id}")

                # Create new agent metrics record
                cursor.execute("""
                    INSERT INTO agent_performance
                    (agent_id, total_responses, sum_response_time, avg_response_time, last_updated)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    agent_id,
                    1,
                    response_time_minutes,
                    response_time_minutes,
                    datetime.now()
                ))
        else:
            logger.warning(f"Failed to create analytics record for agent response, ticket ID {ticket_id}")

        return True

    except Exception as e:
        logger.error(f"Error processing agent performance analytics: {str(e)}")
        return False


def lambda_handler(event, context):
    connection = None
    cursor = None
    processed_records = []

    try:
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        for record in event['Records']:
            record_result = {
                'success': False,
                'record_id': record.get('messageId', 'unknown')
            }

            try:
                # Parse SNS message
                message = json.loads(record['Sns']['Message'])

                # Determine event type and process accordingly
                event_type = message.get('event_type')

                if event_type == 'ticket_resolution':
                    success = process_resolution_analytics(message, cursor)
                elif event_type == 'ticket_creation':
                    success = process_ticket_creation_analytics(message, cursor)
                elif event_type == 'agent_response':
                    success = process_agent_performance_analytics(message, cursor)
                else:
                    logger.warning(f"Unknown analytics event type: {event_type}")
                    success = False

                connection.commit()

                # Log result
                record_result['success'] = success
                record_result['event_type'] = event_type

                if success:
                    logger.info(f"Successfully processed {event_type} analytics")
                else:
                    logger.warning(f"Failed to process {event_type} analytics")

            except Exception as record_error:
                logger.error(f"Error processing analytics record: {str(record_error)}")
                record_result['error'] = str(record_error)

                # Roll back transaction for this record
                connection.rollback()

                # Log error
                log_to_sns(4, 21, 10, 43, record_result,
                           "Support Analytics Error", None)

            # Add result to processed records
            processed_records.append(record_result)

        # Create summary of processing
        success_count = sum(1 for r in processed_records if r.get('success', False))

        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed_count': len(processed_records),
                'success_count': success_count,
                'results': processed_records
            })
        }

    except Exception as e:
        logger.error(f"Lambda execution error: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process support analytics',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()