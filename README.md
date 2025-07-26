# S3 to Clazar Usage Export Script

This script automatically pulls usage metering data from S3 and uploads aggregated data to Clazar on an hourly basis. It maintains state to ensure no data gaps or duplicates, making it suitable for production deployment.

## Prerequisites

### System Requirements
- Python 3.7 or higher
- AWS CLI configured or AWS credentials available
- Network access to S3 and Clazar API

### Python Dependencies
```bash
pip install boto3 requests
```

### AWS Permissions
Your AWS credentials need the following S3 permissions:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

## Installation

### 1. Install Dependencies
```bash
# Activate your virtual environment if using one
source venv/bin/activate

# Install from requirements.txt
pip install -r requirements.txt
```

### 2. Configure AWS Credentials

Choose one of the following methods:

**Option A: AWS CLI Configuration**
```bash
aws configure
```

**Option B: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

## Configuration

### Required Environment Variables

Set these environment variables before running the script:

```bash
# Required Configuration
export S3_BUCKET_NAME="omnistrate-usage-metering-export-demo" # This should match your S3 bucket name
export SERVICE_NAME="Postgres" # This should match the service name in your S3 paths
export ENVIRONMENT_TYPE="PROD" # This should match the environment type in your S3 paths
export PLAN_ID="pt-HJSv20iWX0" # This should match the plan ID in your S3 paths
export CLAZAR_CLIENT_ID="your-clazar-client-id" # Your Clazar client ID
export CLAZAR_CLIENT_SECRET="your-clazar-client-secret" # Your Clazar client secret
export CLAZAR_CLOUD="aws"  # This should be the marketplace cloud (aws, azure, gcp, etc.)

# Optional Configuration (with defaults)
export CLAZAR_API_URL="https://api.clazar.io/metering/"
export STATE_FILE_PATH="./metering_state.json"
export MAX_HOURS_PER_RUN="24"
export DRY_RUN="false" # Set to true for testing without sending data to Clazar
```

### Clazar Dimensions
This script assumes you are charging for the following dimensions and have configured them in Clazar:
- `memory_byte_hours`
- `storage_allocated_byte_hours`
- `cpu_core_hours`

If you are using different dimensions, update the script accordingly to aggregate and send the correct data.

## Running the Script

### Manual Execution

```bash
# Test run (from project directory)
python3 metering_processor.py
```

### Check Logs
The script provides detailed logging. Monitor the output for:
- Successfully processed hours
- Any errors or warnings
- State updates

Example output:
```
2025-07-25 20:15:32,604 - INFO - Processing hour 21/24: 2025-07-23 21:00:00
2025-07-25 20:15:32,604 - INFO - Processing hour: 2025-07-23 21:00:00 for Postgres/PROD/pt-HJSv20iWX0
2025-07-25 20:15:32,662 - INFO - Found 1 subscription files in omnistrate-metering/Postgres/PROD/pt-HJSv20iWX0/2025/07/23/21/
2025-07-25 20:15:32,735 - INFO - Aggregated 3 records into 3 entries
2025-07-25 20:15:32,736 - INFO - Sending 3 metering records to Clazar
2025-07-25 20:15:33,866 - WARNING - Sent data to Clazar with warnings: status=fail. Please check if the dimensions are registered in Clazar.
2025-07-25 20:15:33,866 - INFO - Response: {'results': [{'id': '4a4fefdc-07a9-4b84-a1ee-60c6bb690b12', 'cloud': 'aws', 'contract_id': 'ae641bd1-edf8-4038-bfed-d2ff556c729e', 'dimension': 'cpu_core_hours', 'quantity': '1', 'status': 'fail', 'start_time': '2025-07-23T21:00:00Z', 'end_time': '2025-07-23T22:00:00Z', 'custom_properties': {}}, {'id': '82b5f8f6-b520-4747-80f2-c9e546b63c2b', 'cloud': 'aws', 'contract_id': 'ae641bd1-edf8-4038-bfed-d2ff556c729e', 'dimension': 'storage_allocated_byte_hours', 'quantity': '1', 'status': 'fail', 'start_time': '2025-07-23T21:00:00Z', 'end_time': '2025-07-23T22:00:00Z', 'custom_properties': {}}, {'id': 'ab5133c2-45fe-4af7-83f4-736ac5f31a9a', 'cloud': 'aws', 'contract_id': 'ae641bd1-edf8-4038-bfed-d2ff556c729e', 'dimension': 'memory_byte_hours', 'quantity': '1', 'status': 'fail', 'start_time': '2025-07-23T21:00:00Z', 'end_time': '2025-07-23T22:00:00Z', 'custom_properties': {}}]}
2025-07-25 20:15:33,868 - INFO - Loaded state from metering_state.json
2025-07-25 20:15:33,869 - INFO - Saved state to metering_state.json
2025-07-25 20:15:33,869 - INFO - Loaded state from metering_state.json
2025-07-25 20:15:33,869 - INFO - Processing hour 22/24: 2025-07-23 22:00:00
2025-07-25 20:15:33,869 - INFO - Processing hour: 2025-07-23 22:00:00 for Postgres/PROD/pt-HJSv20iWX0
2025-07-25 20:15:33,940 - INFO - Found 1 subscription files in omnistrate-metering/Postgres/PROD/pt-HJSv20iWX0/2025/07/23/22/
2025-07-25 20:15:34,011 - INFO - Aggregated 3 records into 3 entries
2025-07-25 20:15:34,012 - INFO - Sending 3 metering records to Clazar
2025-07-25 20:15:37,526 - WARNING - Sent data to Clazar with warnings: status=fail. Please check if the dimensions are registered in Clazar.
2025-07-25 20:15:37,526 - INFO - Response: {'results': [{'id': '85ccbe45-e12b-4c46-aadd-a29bfb523c14', 'cloud': 'aws', 'contract_id': 'ae641bd1-edf8-4038-bfed-d2ff556c729e', 'dimension': 'cpu_core_hours', 'quantity': '1', 'status': 'fail', 'start_time': '2025-07-23T22:00:00Z', 'end_time': '2025-07-23T23:00:00Z', 'custom_properties': {}}, {'id': '3238c1b0-2619-4235-9169-01ceeea6edeb', 'cloud': 'aws', 'contract_id': 'ae641bd1-edf8-4038-bfed-d2ff556c729e', 'dimension': 'storage_allocated_byte_hours', 'quantity': '1', 'status': 'fail', 'start_time': '2025-07-23T22:00:00Z', 'end_time': '2025-07-23T23:00:00Z', 'custom_properties': {}}, {'id': '1402b80d-e2c1-4bff-9e02-7cbcbc15e364', 'cloud': 'aws', 'contract_id': 'ae641bd1-edf8-4038-bfed-d2ff556c729e', 'dimension': 'memory_byte_hours', 'quantity': '1', 'status': 'fail', 'start_time': '2025-07-23T22:00:00Z', 'end_time': '2025-07-23T23:00:00Z', 'custom_properties': {}}]}
2025-07-25 20:15:37,527 - INFO - Loaded state from metering_state.json
2025-07-25 20:15:37,528 - INFO - Saved state to metering_state.json
2025-07-25 20:15:37,529 - INFO - Loaded state from metering_state.json
```
