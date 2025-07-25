# S3 to Clazar Usage Export Script

This script automatically pulls usage metering data from S3 and uploads aggregated data to Clazar on an hourly basis. It maintains state to ensure no data gaps or duplicates, making it suitable for production deployment.

## Features

- ✅ **Stateful Processing**: Tracks last processed hour to prevent gaps and duplicates
- ✅ **Automatic Catch-up**: Processes missed hours if the script was down
- ✅ **Data Aggregation**: Combines multiple records per hour per buyer-dimension
- ✅ **Error Recovery**: Robust error handling with detailed logging
- ✅ **Multi-Service Support**: Can handle multiple service configurations
- ✅ **Production Ready**: Designed for reliable hourly cron execution

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
export S3_BUCKET_NAME="omnistrate-usage-metering-export-demo"
export SERVICE_NAME="Postgres"
export ENVIRONMENT_TYPE="PROD"
export PLAN_ID="pt-HJSv20iWX0"
export CLAZAR_CLIENT_ID="your-clazar-client-id"
export CLAZAR_CLIENT_SECRET="your-clazar-client-secret"
export CLAZAR_CLOUD="aws"  # or "azure", "gcp"

# Optional Configuration (with defaults)
export CLAZAR_API_URL="https://api.clazar.io/metering/"
export STATE_FILE_PATH="./metering_state.json"
export MAX_HOURS_PER_RUN="24"
```

### Configuration File Method (Alternative)

Create a configuration file `config.env` in your project directory:

```bash
# config.env
S3_BUCKET_NAME=omnistrate-usage-metering-export-demo
SERVICE_NAME=Postgres
ENVIRONMENT_TYPE=PROD
PLAN_ID=pt-HJSv20iWX0
CLAZAR_API_URL=https://api.clazar.io/metering/
STATE_FILE_PATH=./metering_state.json
MAX_HOURS_PER_RUN=24
```

Then load it before running:
```bash
source config.env
python3 metering_processor.py
```

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
