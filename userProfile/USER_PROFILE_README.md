# User Profile Workflow

## Features

### Synchronous Operations
- **View Profile**: Access personal information, contacts, and settings
- **Update Information**: Modify name, email, phone, and address
- **View Ratings/Reviews**: See feedback given to service providers
- **Manage Preferences**: Control app settings and notifications

### Asynchronous Operations
- **Profile Picture Management**: Upload and process user images
- **Password Management**: Change password and recovery flows
- **Payment Methods**: Add, update, or remove payment options
- **Data Privacy Controls**: Request data export or deletion
- **Activity Monitoring**: Track account activities
- **2FA Management**: Enable/disable two-factor authentication

## Technical Architecture

### Synchronous Functions
1. **viewUserProfile.py**
   - Retrieves core profile data and preferences
   - Handles profile picture URL generation

2. **updateUserInformation.py**
   - Validates and updates personal information
   - Maintains data integrity and format validation

3. **viewUserRatingsReviews.py**
   - Provides paginated history of ratings given
   - Includes rating statistics and filtering

4. **preferencesSettings.py**
   - Manages user preference settings
   - Handles GET/PUT operations for preferences

### Asynchronous Functions
5. **profilePictureUpload1.py** & **profilePictureUpload2.py**
   - Validates and stores image temporarily (Lambda 1)
   - Processes image and creates thumbnails (Lambda 2)

6. **changePassword1.py** & **changePassword2.py**
   - Validates current password and requirements (Lambda 1)
   - Securely updates password and notifies user (Lambda 2)

7. **forgotPassword1.py** & **forgotPassword2.py**
   - Initiates password reset and validates user (Lambda 1)
   - Sends secure reset instructions to user (Lambda 2)

8. **verifyOtpResetPassword.py**
   - Validates OTP and processes password reset
   - Handles security checks for reset attempts

9. **managePaymentMethods1.py** & **managePaymentMethods2.py**
   - Validates payment details (Lambda 1)
   - Securely stores payment information via Stripe (Lambda 2)

10. **privacyDataManagement1.py** & **privacyDataManagement2.py**
    - Validates data requests (Lambda 1)
    - Processes data export or deletion (Lambda 2)

11. **userActivityLogs1.py** & **userActivityLogs2.py** & **getUserActivityLogs.py**
    - Records user activities (Lambda 1)
    - Processes security monitoring (Lambda 2)
    - Retrieves activity history (separate function)

12. **manage2fa1.py** & **manage2fa2.py**
    - Handles 2FA setup and verification (Lambda 1)
    - Activates/deactivates 2FA and notifies user (Lambda 2)

### Shared Components
The `utils.py` file contains common utilities:
- Database connection handling
- Password security functions
- Input validation helpers
- Notification services
- AWS service integrations

### Data Flow
1. Client makes request to API Gateway
2. API Gateway routes to appropriate Lambda
3. For synchronous operations:
   - Lambda processes request directly
   - Returns immediate response to client
4. For asynchronous operations:
   - First Lambda validates request and publishes to SNS
   - Second Lambda performs processing asynchronously
   - User receives notification when complete

