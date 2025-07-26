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
2025-07-24 16:30:00,123 - INFO - Starting processing for Postgres/PROD/pt-HJSv20iWX0
2025-07-24 16:30:00,456 - INFO - No previous processing found, starting from 2025-07-23 16:00:00
2025-07-24 16:30:01,789 - INFO - Processing hour 1/24: 2025-07-23 16:00:00
2025-07-24 16:30:02,012 - INFO - Found 3 subscription files in omnistrate-metering/Postgres/PROD/pt-HJSv20iWX0/2025/07/23/16/
2025-07-24 16:30:02,345 - INFO - Aggregated 150 records into 12 entries
2025-07-24 16:30:02,678 - INFO - Sending 12 metering records to Clazar
2025-07-24 16:30:03,901 - INFO - Successfully sent data to Clazar
2025-07-24 16:30:03,902 - INFO - Saved state to ./metering_state.json
```
