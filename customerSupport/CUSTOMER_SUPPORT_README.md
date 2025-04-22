# Customer Support System - Technical Documentation

## System Overview

This documentation outlines the customer support workflow implementation.

## Architecture Patterns

The system implements two primary architectural patterns:

### 1. Basic Process (Synchronous)
- **Flow**: API Gateway → Lambda Function → API Gateway
- **Use Case**: Immediate data retrieval and simple operations
- **Characteristics**: Quick response time, direct feedback to users

### 2. Standard Process (Asynchronous)
- **Flow**: API Gateway → Lambda 1 → SNS → Lambda 2 → API Gateway
- **Use Case**: Complex operations, background processing, notifications
- **Characteristics**: Non-blocking, better scalability, decoupled services

## Workflow Components

### 1. Submit Support Ticket (Async)
- **Purpose**: Allow users to create support tickets with proper categorization
- **Process Type**: Standard Process (Asynchronous)
- **Implementation**: `submitSupportTicket1.py` & `submitSupportTicket2.py`
- **Features**:
  - Category-based routing to appropriate departments
  - Special handling for "other" category tickets
  - Attachment support
  - Email confirmation to users

### 2. View Submitted Ticket Status (Sync)
- **Purpose**: Allow users to check real-time status of their tickets
- **Process Type**: Basic Process (Synchronous)
- **Implementation**: `viewTicketStatus.py`

### 3. Update Existing Ticket (Async)
- **Purpose**: Allow users to add information to existing tickets
- **Process Type**: Standard Process (Asynchronous)
- **Implementation**: `updateTicket1.py` & `updateTicket2.py`
- **Features**:
  - Additional information updates
  - Comment support
  - Attachment support
  - Notification to support team

### 4. Escalation Handling & Management (Async)
- **Purpose**: Automatically escalate tickets to higher support tiers
- **Process Type**: Standard Process (Asynchronous)
- **Implementation**: `escalationHandler1.py` & `escalationHandler2.py`
- **Trigger**: CloudWatch scheduled event
- **Features**:
  - Time-based escalation rules
  - Priority-based escalation
  - Multi-level escalation (team lead, manager, director)
  - Notification to all parties

### 5. Ticket Resolution Confirmation (Async)
- **Purpose**: Allow users to confirm resolution and provide feedback
- **Process Type**: Standard Process (Asynchronous)
- **Implementation**: `ticketResolution1.py` & `ticketResolution2.py`
- **Features**:
  - Satisfaction rating collection
  - Feedback text support
  - Analytics integration
  - Agent notification

### 6. Support Interaction Analytics (Async)
- **Purpose**: Analyze customer support interactions for performance metrics
- **Process Type**: Standard Process (Asynchronous)
- **Implementation**: `supportAnalytics.py`
- **Trigger**: SNS events from various support activities
- **Features**:
  - Resolution time tracking
  - Satisfaction rating aggregation
  - Department performance metrics
  - Agent performance metrics

### 7. Notification on Ticket Updates (Async)
- **Purpose**: Automatically notify users when support agents update their tickets
- **Process Type**: Standard Process (Asynchronous)
- **Implementation**: `agentUpdateTicket1.py` & `agentUpdateTicket2.py`
- **Trigger**: Support agent updates to tickets
- **Features**:
  - Email notifications of status changes
  - In-app notification creation
  - Public comment notifications
  - Update tracking in ticket history

### 8. Knowledge Base Article Feedback (Async)
- **Purpose**: Collect user feedback on help articles for improvement
- **Process Type**: Standard Process (Asynchronous)
- **Implementation**: `knowledgeBaseFeedback1.py` & `knowledgeBaseFeedback2.py`
- **Features**:
  - Helpfulness rating
  - Numerical rating (1-5)
  - Comment support
  - Content review triggering for poor ratings

### 9. Scheduled Maintenance Notifications (Async)
- **Purpose**: Inform users about planned system maintenance
- **Process Type**: Standard Process (Asynchronous)
- **Implementation**: `maintenanceNotifications1.py` & `maintenanceNotifications2.py`
- **Features**:
  - Multi-channel notifications (email, in-app)
  - Service-specific targeting
  - Severity levels
  - Customizable messaging

## Common Utilities

The system includes shared utilities in `utils.py`:

- `get_secrets()` - Retrieve configuration from AWS Secrets Manager
- `get_db_connection()` - Create and manage database connections
- `log_to_sns()` - Standardized logging format through SNS
- `send_email_via_ses()` - Email delivery via Amazon SES


## Error Handling and Logging

All Lambda functions implement consistent error handling patterns:

1. Database query validation with conditional checks
2. Try/except blocks with appropriate error messages
3. Transaction management (commit/rollback)
4. Standardized logging to CloudWatch
5. Error reporting via SNS for monitoring
6. Appropriate HTTP status codes in responses

## Security Considerations

- All database credentials stored in AWS Secrets Manager
- Admin-only endpoints require role verification
- User authentication via API Gateway (not shown in implementation)
- Database connection pooling for efficiency
- Parameterized SQL queries to prevent injection

## Monitoring and Maintenance

- CloudWatch logs configured for all Lambda functions
- Error tracking via SNS notifications
- Performance metrics available through support analytics
- Scheduled maintenance process for system updates