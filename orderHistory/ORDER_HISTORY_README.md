# Order History Workflow

## Features

### Synchronous Operations
- **View Order History**: List of completed and canceled service orders
- **Filter and Search**: Filter by time period, service type, status
- **View Detailed Information**: Comprehensive details about specific orders
- **Service Time Updates**: Track acceptance and completion times
- **Like Thank-You Notes**: Acknowledge provider thank-you messages
- **Rating/Tipping Status**: Check if user has rated or tipped
- **View Photos**: Access before-and-after service documentation

### Asynchronous Operations
- **Submit Complaints**: Report issues with completed services
- **Download Invoices/Receipts**: Generate order documentation
- **Review and Rate**: Submit feedback for completed services
- **Request Re-Service**: Reorder based on previous orders

## Technical Architecture

### Synchronous Functions
1. **viewOrderHistory.py**
   - Returns paginated list of orders with basic information
   - Supports different views for customers vs. providers

2. **filterSearchOrderHistory.py**
   - Applies complex filters to order history queries
   - Handles date ranges, service types, and text search

3. **viewDetailedOrderInfo.py**
   - Fetches comprehensive order details including pricing
   - Includes related data (provider info, status history)

4. **updateAcceptCompleteTimes.py**
   - Updates service milestone timestamps
   - Triggers status changes based on updates

5. **likeThankYouNote.py**
   - Processes message acknowledgments
   - Updates message metadata and notifies sender

6. **checkRatingStatus.py**
   - Verifies if ratings exist for an order
   - Returns rating details and statistics

7. **checkTippingStatus.py**
   - Checks if tips have been processed
   - Returns payment details for tips

8. **viewBeforeAfterPhotos.py**
   - Retrieves service documentation photos
   - Generates secure time-limited URLs

### Asynchronous Functions
9. **submitComplaint1.py** & **submitComplaint2.py**
   - Records customer complaints (Lambda 1)
   - Creates support tickets and notifies staff (Lambda 2)

10. **downloadInvoice1.py** & **downloadInvoice2.py**
    - Initiates document generation request (Lambda 1)
    - Creates PDF documents and notifies user (Lambda 2)

11. **reviewRateOrder1.py** & **reviewRateOrder2.py**
    - Captures feedback and validates input (Lambda 1)
    - Processes ratings and updates statistics (Lambda 2)

12. **requestReService1.py** & **requestReService2.py**
    - Creates new request based on previous order (Lambda 1)
    - Notifies providers and schedules service (Lambda 2)

13. **exportOrderHistory.py**, **processExport.py**, & **downloadExport.py**
    - Initiates export request for order history (Lambda 1)
    - Processes and generates export files (Lambda 2)
    - Handles download of generated export files (Lambda 3)

### Data Flow
1. Client makes request to API Gateway
2. API Gateway triggers appropriate Lambda function
3. For synchronous operations:
   - Lambda connects to database
   - Processes request and returns response directly
4. For asynchronous operations:
   - First Lambda validates request and publishes to SNS
   - Second Lambda processes request asynchronously
   - User receives confirmation via email/push notification

## Using the Generic Lookup Function

A generic lookup function (`lookUp.py`) has been implemented to handle simple SELECT queries. This function can be used to simplify database operations for straightforward data retrieval needs.

### Capabilities of lookUp.py
- Retrieve data from a single table
- Select specific fields
- Apply simple WHERE conditions using equality (=) with AND logic
- Order results by a single column

### Suitable Operations for lookUp.py
Some functions contain simple SELECT queries that could be replaced with calls to the lookup function:

1. **likeThankYouNote.py**
   - Checking if a message is already liked:
   ```sql
   SELECT likeid FROM message_likes WHERE messageid = %s AND userid = %s
   ```

2. **updateAcceptCompleteTimes.py**
   - Verifying TidySP assignment to an order:
   ```sql
   SELECT orderid, status FROM orders WHERE orderid = %s AND tidyspid = %s
   ```

3. Other simple queries that don't involve joins, OR conditions, or complex filtering
