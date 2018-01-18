# Plugin - Freshdesk to S3

This plugin moves data from the [Freshdesk](https://developers.freshdesk.com/api) API to S3. Impelemented for agents, companies, contacts, conversations, groups, roles, satisfaction-ratings, tickets and time-entries
## Hooks
### FreshdeskHook
This hook handles the authentication and request to Freshdesk. Inherits from HttpHook.

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

## Operators
### FreshdeskToS3Operator
This operator composes the logic for this plugin. It fetches a specific endpoint and saves the result in a S3 Bucket, under a specified key, in
njson format. The parameters it can accept include the following.

- `freshdesk_conn_id`: The Airflow id used to store the Freshdesk credentials.
- `freshdesk_endpoint`: The endpoint to retrive data from.
- `updated_at`: *optional* string with a date, used as a replication key
- `s3_conn_id`: S3 connection id from Airflow.  
- `s3_bucket`: The output s3 bucket.  
- `s3_key`: The input s3 key.  