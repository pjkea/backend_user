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
ESCALATION_TOPIC_ARN = secrets["ESCALATION_TOPIC_ARN"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Create database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Current time
        current_time = datetime.now()

        # Define escalation rules
        escalation_rules = [
            {
                'status': 'New',
                'priority': 'High',
                'age_hours': 2,
                'escalation_level': 1
            },
            {
                'status': 'New',
                'priority': 'Medium',
                'age_hours': 8,
                'escalation_level': 1
            },
            {
                'status': 'New',
                'priority': 'Low',
                'age_hours': 24,
                'escalation_level': 1
            },
            {
                'status': 'In Progress',
                'priority': 'High',
                'age_hours': 24,
                'escalation_level': 2
            },
            {
                'status': 'In Progress',
                'priority': 'Medium',
                'age_hours': 48,
                'escalation_level': 2
            },
            {
                'status': 'Pending Agent',
                'priority': 'High',
                'age_hours': 4,
                'escalation_level': 1
            }
        ]

        escalated_tickets = []

        # Process each escalation rule
        for rule in escalation_rules:
            status = rule['status']
            priority = rule['priority']
            age_hours = rule['age_hours']
            escalation_level = rule['escalation_level']

            # Calculate cutoff time for this rule
            cutoff_time = current_time - timedelta(hours=age_hours)

            # Find tickets that match escalation criteria
            cursor.execute("""
                SELECT t.ticket_id, t.user_id, t.subject, t.created_at, t.updated_at, 
                       t.status, t.priority, t.department_id, d.department_name
                FROM support_tickets t
                JOIN support_departments d ON t.department_id = d.department_id
                WHERE t.status = %s
                AND t.priority = %s
                AND t.updated_at < %s
                AND (
                    SELECT COUNT(*) FROM ticket_history 
                    WHERE ticket_id = t.ticket_id 
                    AND action = 'Escalated' 
                    AND action_timestamp > %s
                ) = 0
            """, (status, priority, cutoff_time, cutoff_time - timedelta(hours=24)))

            tickets = cursor.fetchall()

            if tickets:
                logger.info(f"Found {len(tickets)} tickets matching escalation rule: {status}/{priority}/{age_hours}h")
            else:
                logger.info(f"No tickets found matching escalation rule: {status}/{priority}/{age_hours}h")

            # Process each ticket for escalation
            for ticket in tickets:
                ticket_id = ticket['ticket_id']

                # Determine escalation target based on level
                if escalation_level == 1:
                    # First level escalation - to team lead
                    new_department_id = ticket['department_id']  # Same department, but will notify team lead
                    escalation_target = "Team Lead"
                elif escalation_level == 2:
                    # Second level escalation - to department manager
                    new_department_id = ticket['department_id']  # Same department but higher level
                    escalation_target = "Department Manager"
                else:
                    # Highest level escalation - to customer support director
                    new_department_id = None  # Special handling at director level
                    escalation_target = "Support Director"

                # Update ticket with escalation note
                cursor.execute("""
                    UPDATE support_tickets
                    SET priority = 
                        CASE 
                            WHEN priority = 'Low' THEN 'Medium'
                            WHEN priority = 'Medium' THEN 'High'
                            ELSE 'High'
                        END,
                    status = 'Escalated',
                    updated_at = %s
                    WHERE ticket_id = %s
                """, (current_time, ticket_id))

                # Add escalation history entry
                cursor.execute("""
                    INSERT INTO ticket_history
                    (ticket_id, action, action_by, action_timestamp, notes)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    ticket_id,
                    'Escalated',
                    'system',
                    current_time,
                    json.dumps({
                        'escalation_level': escalation_level,
                        'escalation_target': escalation_target,
                        'reason': f"Ticket in {status} status with {priority} priority for over {age_hours} hours",
                        'previous_priority': ticket['priority']
                    })
                ))

                # Prepare ticket data for SNS
                ticket_data = dict(ticket)
                ticket_data['escalation_level'] = escalation_level
                ticket_data['escalation_target'] = escalation_target
                ticket_data['escalation_timestamp'] = current_time.isoformat()

                # Publish to SNS for notification processing
                sns_client.publish(
                    TopicArn=ESCALATION_TOPIC_ARN,
                    Message=json.dumps(ticket_data),
                    Subject=f"Ticket Escalation: {ticket_id}"
                )

                # Add to list of escalated tickets
                escalated_tickets.append({
                    'ticket_id': ticket_id,
                    'status': status,
                    'priority': ticket['priority'],
                    'new_priority': 'High' if ticket['priority'] == 'High' else (
                        'Medium' if ticket['priority'] == 'Low' else 'High'),
                    'escalation_level': escalation_level,
                    'escalation_target': escalation_target
                })

        # Commit all changes
        connection.commit()

        # Log escalation activity
        log_data = {
            'escalated_tickets_count': len(escalated_tickets),
            'escalated_tickets': escalated_tickets
        }
        log_to_sns(1, 21, 7, 1, log_data, "Ticket Escalation Process", None)

        logger.info(f"Successfully processed escalations for {len(escalated_tickets)} tickets")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Escalation process completed successfully',
                'escalated_tickets_count': len(escalated_tickets),
                'escalated_tickets': escalated_tickets
            })
        }

    except Exception as e:
        logger.error(f"Error in escalation process: {str(e)}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error
        error_data = {
            'error': str(e)
        }
        log_to_sns(4, 21, 7, 43, error_data, "Ticket Escalation Process Error", None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to process ticket escalations',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()