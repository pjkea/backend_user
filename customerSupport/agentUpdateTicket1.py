import json
import boto3
import logging
from datetime import datetime
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
AGENT_TICKET_UPDATE_TOPIC_ARN = secrets["AGENT_TICKET_UPDATE_TOPIC_ARN"]
SNS_LOGGING_TOPIC_ARN = secrets["SNS_LOGGING_TOPIC_ARN"]


def lambda_handler(event, context):
    connection = None
    cursor = None

    try:
        # Extract agent ID from query parameters
        agent_id = event.get('queryStringParameters', {}).get('agentid')
        if not agent_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: agentid'})
            }

        # Parse request body
        body = json.loads(event.get('body', '{}'))
        ticket_id = body.get('ticket_id')

        if not ticket_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Missing required parameter: ticket_id'})
            }

        # Extract update parameters
        status_update = body.get('status')
        internal_notes = body.get('internal_notes')
        public_comment = body.get('public_comment')
        priority_update = body.get('priority')
        department_update = body.get('department_id')

        # Validate that at least one update parameter is provided
        if not any([status_update, internal_notes, public_comment, priority_update, department_update]):
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'At least one update field is required (status, internal_notes, public_comment, priority, or department_id)'
                })
            }

        # Create database connection
        connection = get_db_connection()
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        # Verify agent permissions
        cursor.execute("""
            SELECT role FROM users 
            WHERE userid = %s AND (role = 'agent' OR role = 'admin' OR role = 'manager')
        """, (agent_id,))

        agent_role = cursor.fetchone()

        if agent_role:
            logger.info(f"Agent role verified for agent ID {agent_id}")
        else:
            logger.warning(f"No agent role found for ID {agent_id}")
            return {
                'statusCode': 403,
                'body': json.dumps({'message': 'Unauthorized: Only support agents can update tickets'})
            }

        # Verify ticket exists
        cursor.execute("""
            SELECT t.ticket_id, t.user_id, t.subject, t.status, t.priority, 
                   t.department_id, d.department_name 
            FROM support_tickets t
            JOIN support_departments d ON t.department_id = d.department_id
            WHERE t.ticket_id = %s
        """, (ticket_id,))

        ticket = cursor.fetchone()

        if ticket:
            logger.info(f"Ticket found for ticket ID {ticket_id}")
        else:
            logger.warning(f"No ticket found for ticket ID {ticket_id}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Ticket not found'})
            }

        # Store original values for change tracking
        original_values = {
            'status': ticket['status'],
            'priority': ticket['priority'],
            'department_id': ticket['department_id']
        }

        # Initialize update parts
        updates = []
        values = []

        # Track changes for notification
        changes = {}

        # Process status update
        if status_update and status_update != ticket['status']:
            updates.append("status = %s")
            values.append(status_update)
            changes['status'] = {
                'from': ticket['status'],
                'to': status_update
            }

        # Process priority update
        if priority_update and priority_update != ticket['priority']:
            updates.append("priority = %s")
            values.append(priority_update)
            changes['priority'] = {
                'from': ticket['priority'],
                'to': priority_update
            }

        # Process department update
        if department_update and str(department_update) != str(ticket['department_id']):
            # Verify department exists
            cursor.execute("SELECT department_name FROM support_departments WHERE department_id = %s",
                           (department_update,))
            dept = cursor.fetchone()

            if dept:
                logger.info(f"Department found for department ID {department_update}")
                updates.append("department_id = %s")
                values.append(department_update)
                changes['department'] = {
                    'from': {
                        'id': ticket['department_id'],
                        'name': ticket['department_name']
                    },
                    'to': {
                        'id': department_update,
                        'name': dept['department_name']
                    }
                }
            else:
                logger.warning(f"No department found for department ID {department_update}")
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Invalid department ID'})
                }

        # Always update the updated_at timestamp
        updates.append("updated_at = %s")
        values.append(datetime.now())

        # Update the ticket if there are field updates
        if updates:
            # Add ticket_id to values for WHERE clause
            values.append(ticket_id)

            # Prepare SQL update statement
            sql = f"""
                UPDATE support_tickets 
                SET {', '.join(updates)} 
                WHERE ticket_id = %s
            """

            # Execute update
            cursor.execute(sql, values)

            logger.info(f"Ticket {ticket_id} updated with field changes")

        # Add agent comment if provided
        comment_id = None
        if public_comment:
            cursor.execute("""
                INSERT INTO ticket_comments
                (ticket_id, user_id, comment_text, created_at, is_staff)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING comment_id
            """, (
                ticket_id,
                agent_id,
                public_comment,
                datetime.now(),
                True  # is_staff flag
            ))

            result = cursor.fetchone()
            if result:
                logger.info(f"Public comment added to ticket {ticket_id}")
                comment_id = result['comment_id']
                changes['public_comment'] = {
                    'comment_id': comment_id,
                    'text': public_comment
                }
            else:
                logger.warning(f"Failed to add public comment to ticket {ticket_id}")

        # Add internal notes if provided
        internal_note_id = None
        if internal_notes:
            cursor.execute("""
                INSERT INTO ticket_internal_notes
                (ticket_id, agent_id, note_text, created_at)
                VALUES (%s, %s, %s, %s)
                RETURNING note_id
            """, (
                ticket_id,
                agent_id,
                internal_notes,
                datetime.now()
            ))

            result = cursor.fetchone()
            if result:
                logger.info(f"Internal note added to ticket {ticket_id}")
                internal_note_id = result['note_id']
            else:
                logger.warning(f"Failed to add internal note to ticket {ticket_id}")

        # Create history entry for the update
        cursor.execute("""
            INSERT INTO ticket_history
            (ticket_id, action, action_by, action_timestamp, notes)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            ticket_id,
            'Agent Update',
            agent_id,
            datetime.now(),
            json.dumps({
                'changes': changes,
                'internal_note_added': internal_notes is not None,
                'public_comment_added': public_comment is not None
            })
        ))

        # Prepare message for SNS notification only if there are user-visible changes
        if changes or public_comment:
            update_message = {
                'ticket_id': ticket_id,
                'subject': ticket['subject'],
                'user_id': ticket['user_id'],
                'agent_id': agent_id,
                'timestamp': datetime.now().isoformat(),
                'changes': changes,
                'public_comment': public_comment,
                'public_comment_id': comment_id
            }

            # Publish to SNS for notification processing
            sns_client.publish(
                TopicArn=AGENT_TICKET_UPDATE_TOPIC_ARN,
                Message=json.dumps(update_message),
                Subject=f"Agent Update: Ticket {ticket_id}"
            )

            logger.info(f"Update notification sent to SNS for ticket {ticket_id}")

        # Commit all database changes
        connection.commit()

        # Log successful update
        log_data = {
            'ticket_id': ticket_id,
            'changes': changes,
            'public_comment_added': public_comment is not None,
            'internal_note_added': internal_notes is not None
        }
        log_to_sns(1, 21, 8, 1, log_data, "Agent Ticket Update", agent_id)

        logger.info(f"Successfully processed agent update for ticket {ticket_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Ticket updated successfully',
                'ticket_id': ticket_id,
                'changes': changes,
                'public_comment_id': comment_id,
                'internal_note_id': internal_note_id
            })
        }

    except Exception as e:
        logger.error(f"Error updating ticket: {str(e)}")

        # Rollback transaction if necessary
        if connection:
            connection.rollback()

        # Log error
        error_data = {
            'ticket_id': ticket_id if 'ticket_id' in locals() else None,
            'agent_id': agent_id if 'agent_id' in locals() else None,
            'error': str(e)
        }
        log_to_sns(4, 21, 8, 43, error_data, "Agent Ticket Update Error",
                   agent_id if 'agent_id' in locals() else None)

        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to update ticket',
                'error': str(e)
            })
        }

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()