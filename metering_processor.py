#!/usr/bin/env python3
"""
S3 to Clazar Usage Metering Script

This script pulls usage metering data from S3 and uploads aggregated data to Clazar.
It processes data hourly and ensures only one metering record per hour per buyer-dimension combo.
"""

import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import requests
from botocore.exceptions import ClientError, NoCredentialsError


class MeteringProcessor:
    def __init__(self, bucket_name: str, state_file_path: str = "metering_state.json", 
                 clazar_api_url: str = "https://api.clazar.io/metering/", dry_run: bool = False,
                 access_token: str = None, cloud: str = "aws"):
        """
        Initialize the metering processor.
        
        Args:
            bucket_name: S3 bucket name containing metering data
            state_file_path: Path to the state file that tracks last processed hour
            clazar_api_url: Clazar API endpoint URL
            dry_run: If True, skip actual API calls and only log payloads
            access_token: Clazar access token for authentication
            cloud: Cloud name (e.g., 'aws', 'azure', 'gcp')
        """
        self.bucket_name = bucket_name
        self.state_file_path = Path(state_file_path)
        self.clazar_api_url = clazar_api_url
        self.dry_run = dry_run
        self.access_token = access_token
        self.cloud = cloud
        self.s3_client = boto3.client('s3')
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def load_state(self) -> Dict:
        """
        Load the processing state from the state file.
        
        Returns:
            Dictionary containing the state information
        """
        if not self.state_file_path.exists():
            self.logger.info("State file not found, initializing with default state")
            return {}
        
        try:
            with open(self.state_file_path, 'r') as f:
                state = json.load(f)
            self.logger.info(f"Loaded state from {self.state_file_path}")
            return state
        except (json.JSONDecodeError, IOError) as e:
            self.logger.error(f"Error loading state file: {e}")
            return {}

    def save_state(self, state: Dict):
        """
        Save the processing state to the state file.
        
        Args:
            state: Dictionary containing the state information
        """
        try:
            # Ensure the directory exists
            self.state_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.state_file_path, 'w') as f:
                json.dump(state, f, indent=2)
            self.logger.info(f"Saved state to {self.state_file_path}")
        except IOError as e:
            self.logger.error(f"Error saving state file: {e}")

    def get_service_key(self, service_name: str, environment_type: str, plan_id: str) -> str:
        """
        Generate a unique key for a service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            Unique service key
        """
        return f"{service_name}:{environment_type}:{plan_id}"

    def get_last_processed_hour(self, service_name: str, environment_type: str, 
                               plan_id: str) -> Optional[datetime]:
        """
        Get the last processed hour for a specific service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            Last processed hour as datetime object, or None if never processed
        """
        state = self.load_state()
        service_key = self.get_service_key(service_name, environment_type, plan_id)
        
        if service_key not in state:
            return None
        
        try:
            last_processed_str = state[service_key]['last_processed_hour']
            return datetime.fromisoformat(last_processed_str.replace('Z', '+00:00')).replace(tzinfo=None)
        except (KeyError, ValueError) as e:
            self.logger.error(f"Error parsing last processed hour for {service_key}: {e}")
            return None

    def update_last_processed_hour(self, service_name: str, environment_type: str, 
                                  plan_id: str, processed_hour: datetime):
        """
        Update the last processed hour for a specific service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            processed_hour: The hour that was just processed
        """
        state = self.load_state()
        service_key = self.get_service_key(service_name, environment_type, plan_id)
        
        if service_key not in state:
            state[service_key] = {}
        
        state[service_key]['last_processed_hour'] = processed_hour.isoformat() + 'Z'
        state[service_key]['last_updated'] = datetime.utcnow().isoformat() + 'Z'
        
        self.save_state(state)

    def get_next_hour_to_process(self, service_name: str, environment_type: str, 
                                plan_id: str) -> Optional[datetime]:
        """
        Get the next hour that needs to be processed.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            Next hour to process, or None if caught up
        """
        last_processed = self.get_last_processed_hour(service_name, environment_type, plan_id)
        current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        
        if last_processed is None:
            # If never processed, start from 24 hours ago to avoid processing incomplete current hour
            start_hour = current_hour - timedelta(hours=24)
            self.logger.info(f"No previous processing found, starting from {start_hour}")
            return start_hour
        
        next_hour = last_processed + timedelta(hours=1)
        
        # Don't process the current hour as it might be incomplete
        if next_hour >= current_hour:
            self.logger.info("Caught up with current hour, no processing needed")
            return None
        
        return next_hour

    def get_hourly_s3_prefix(self, service_name: str, environment_type: str, 
                           plan_id: str, target_hour: datetime) -> str:
        """
        Generate S3 prefix for a specific hour.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type (e.g., PROD, DEV)
            plan_id: Plan ID
            target_hour: Target hour as datetime object
            
        Returns:
            S3 prefix string
        """
        return (f"omnistrate-metering/{service_name}/{environment_type}/"
                f"{plan_id}/{target_hour.year:04d}/{target_hour.month:02d}/"
                f"{target_hour.day:02d}/{target_hour.hour:02d}/")

    def list_subscription_files(self, prefix: str) -> List[str]:
        """
        List all subscription JSON files in the given S3 prefix.
        
        Args:
            prefix: S3 prefix to search
            
        Returns:
            List of S3 object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                self.logger.warning(f"No objects found with prefix: {prefix}")
                return []
            
            # Filter for JSON files
            json_files = [
                obj['Key'] for obj in response['Contents'] 
                if obj['Key'].endswith('.json')
            ]
            
            self.logger.info(f"Found {len(json_files)} subscription files in {prefix}")
            return json_files
            
        except ClientError as e:
            self.logger.error(f"Error listing S3 objects: {e}")
            return []

    def read_s3_json_file(self, key: str) -> List[Dict]:
        """
        Read and parse a JSON file from S3.
        
        Args:
            key: S3 object key
            
        Returns:
            List of usage records
        """
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            
            self.logger.debug(f"Read {len(data)} records from {key}")
            return data
            
        except ClientError as e:
            self.logger.error(f"Error reading S3 file {key}: {e}")
            return []
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing JSON from {key}: {e}")
            return []

    def aggregate_usage_data(self, usage_records: List[Dict]) -> Dict[Tuple[str, str], float]:
        """
        Aggregate usage data by externalPayerId and dimension.
        
        Args:
            usage_records: List of usage records
            
        Returns:
            Dictionary with (externalPayerId, dimension) as key and total usage as value
        """
        aggregated_data = defaultdict(int)
        
        for record in usage_records:
            external_payer_id = record.get('externalPayerId')
            dimension = record.get('dimension')
            value = record.get('value', 0)
            
            if not external_payer_id or not dimension:
                self.logger.warning(f"Skipping record with missing data: {record}")
                continue
            
            key = (external_payer_id, dimension)
            aggregated_data[key] += int(value)
        
        self.logger.info(f"Aggregated {len(usage_records)} records into {len(aggregated_data)} entries")
        return dict(aggregated_data)

    def send_to_clazar(self, aggregated_data: Dict[Tuple[str, str], float], 
                      start_time: datetime, end_time: datetime) -> bool:
        """
        Send aggregated usage data to Clazar.
        
        Args:
            aggregated_data: Aggregated usage data
            start_time: Start time for the metering period
            end_time: End time for the metering period
            
        Returns:
            True if successful, False otherwise
        """
        if not aggregated_data:
            self.logger.info("No data to send to Clazar")
            return True
        
        # Prepare the payload
        metering_records = []
        for (external_payer_id, dimension), quantity in aggregated_data.items():
            record = {
                "cloud": self.cloud,
                "contract_id": external_payer_id,
                "dimension": dimension,
                "start_time": start_time.isoformat() + "Z",
                "end_time": end_time.isoformat() + "Z",
                "quantity": str(quantity)
            }
            metering_records.append(record)
        
        payload = {"request": metering_records}
        
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "Authorization": f"Bearer {self.access_token}" if self.access_token else ""
        }
        
        if not self.access_token and not self.dry_run:
            self.logger.error("Access token is required for sending data to Clazar")
            return False
        
        try:
            self.logger.info(f"Sending {len(metering_records)} metering records to Clazar")
            
            if self.dry_run:
                self.logger.info("DRY RUN MODE: Would send the following payload to Clazar:")
                self.logger.info(f"URL: {self.clazar_api_url}")
                self.logger.info(f"Headers: {headers}")
                self.logger.info(f"Payload: {json.dumps(payload, indent=2)}")
                self.logger.info("DRY RUN MODE: Skipping actual API call")
                return True
            
            response = requests.post(self.clazar_api_url, json=payload, headers=headers)
            
            for result in response.results:
                if "code" in result:
                    self.logger.error(f"Clazar API error: {result['code']} - {result.get('message', '')}")
                    return False
                elif "status" in result and result["status"] != "success":
                    self.logger.error(f"Sent data to Clazar with warnings: status={result['status']}. Please check if the dimensions are registered in Clazar.")
                    self.logger.info(f"Response: {response.json()}")
                    return True
                else:
                    self.logger.info("Successfully sent data to Clazar")
                    self.logger.info(f"Response: {response.json()}")
                    return True
                
        except requests.RequestException as e:
            self.logger.error(f"Error sending data to Clazar: {e}")
            return False

    def process_hour(self, service_name: str, environment_type: str, 
                    plan_id: str, target_hour: datetime) -> bool:
        """
        Process usage data for a specific hour.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            target_hour: Target hour to process
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info(f"Processing hour: {target_hour} for {service_name}/{environment_type}/{plan_id}")
        
        # Get S3 prefix for the hour
        prefix = self.get_hourly_s3_prefix(service_name, environment_type, plan_id, target_hour)
        
        # List all subscription files
        subscription_files = self.list_subscription_files(prefix)
        
        if not subscription_files:
            self.logger.info(f"No subscription files found for {target_hour}")
            return True
        
        # Read and aggregate all usage data
        all_usage_records = []
        for file_key in subscription_files:
            usage_records = self.read_s3_json_file(file_key)
            all_usage_records.extend(usage_records)
        
        if not all_usage_records:
            self.logger.info(f"No usage records found for {target_hour}")
            return True
        
        # Aggregate the data
        aggregated_data = self.aggregate_usage_data(all_usage_records)
        
        # Define the time window (hour boundary)
        start_time = target_hour.replace(minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(hours=1)
        
        # Send to Clazar
        return self.send_to_clazar(aggregated_data, start_time, end_time)

    def process_pending_hours(self, service_name: str, environment_type: str, 
                             plan_id: str, max_hours: int = 24) -> bool:
        """
        Process all pending hours for a specific service configuration.
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            max_hours: Maximum number of hours to process in one run
            
        Returns:
            True if all processing was successful, False otherwise
        """
        self.logger.info(f"Starting processing for {service_name}/{environment_type}/{plan_id}")
        
        processed_count = 0
        all_successful = True
        
        while processed_count < max_hours:
            next_hour = self.get_next_hour_to_process(service_name, environment_type, plan_id)
            
            if next_hour is None:
                self.logger.info("No more hours to process, caught up!")
                break
            
            self.logger.info(f"Processing hour {processed_count + 1}/{max_hours}: {next_hour}")
            
            success = self.process_hour(service_name, environment_type, plan_id, next_hour)
            
            if success:
                # Update state only if processing was successful
                self.update_last_processed_hour(service_name, environment_type, plan_id, next_hour)
                processed_count += 1
            else:
                self.logger.error(f"Failed to process hour {next_hour}, stopping")
                all_successful = False
                break
        
        self.logger.info(f"Processed {processed_count} hours. Success: {all_successful}")
        return all_successful

    def process_current_hour(self, service_name: str, environment_type: str, plan_id: str) -> bool:
        """
        Process usage data for the current hour (deprecated - use process_pending_hours instead).
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.warning("process_current_hour is deprecated, use process_pending_hours instead")
        current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        return self.process_hour(service_name, environment_type, plan_id, current_hour)

    def process_previous_hour(self, service_name: str, environment_type: str, plan_id: str) -> bool:
        """
        Process usage data for the previous hour (deprecated - use process_pending_hours instead).
        
        Args:
            service_name: Name of the service
            environment_type: Environment type
            plan_id: Plan ID
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.warning("process_previous_hour is deprecated, use process_pending_hours instead")
        previous_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        return self.process_hour(service_name, environment_type, plan_id, previous_hour)


def main():
    """Main function to run the metering processor."""
    
    # Configuration - these should be set via environment variables or config file
    BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'omnistrate-usage-metering-export-demo')
    SERVICE_NAME = os.getenv('SERVICE_NAME', 'Postgres')
    ENVIRONMENT_TYPE = os.getenv('ENVIRONMENT_TYPE', 'PROD')
    PLAN_ID = os.getenv('PLAN_ID', 'pt-HJSv20iWX0')
    CLAZAR_CLIENT_ID = os.getenv('CLAZAR_CLIENT_ID', '')
    CLAZAR_CLIENT_SECRET = os.getenv('CLAZAR_CLIENT_SECRET', '')
    CLAZAR_API_URL = os.getenv('CLAZAR_API_URL', 'https://api.clazar.io/metering/')
    STATE_FILE_PATH = os.getenv('STATE_FILE_PATH', 'metering_state.json')
    MAX_HOURS_PER_RUN = int(os.getenv('MAX_HOURS_PER_RUN', '24'))
    DRY_RUN = os.getenv('DRY_RUN', 'false').lower() in ('true', '1', 'yes')
    CLAZAR_CLOUD = os.getenv('CLAZAR_CLOUD', 'aws')
    
    # Validate required environment variables
    if not all([BUCKET_NAME, SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID]):
        print("Error: Missing required configuration. Please set environment variables:")
        print("S3_BUCKET_NAME, SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID")
        sys.exit(1)
    
    try:
        # Authenticate with Clazar
        url = "https://api.clazar.io/authenticate/"

        payload = {
            "client_id": CLAZAR_CLIENT_ID,
            "client_secret": CLAZAR_CLIENT_SECRET
        }
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)
        if response.status_code != 200:
            print(f"Error authenticating with Clazar: {response.status_code} - {response.text}")
            sys.exit(1)

        access_token = response.json().get("access_token")
        if not access_token:
            print("Error: No access token received from Clazar")
            sys.exit(1)

        # Initialize the processor
        processor = MeteringProcessor(BUCKET_NAME, STATE_FILE_PATH, CLAZAR_API_URL, DRY_RUN, access_token, CLAZAR_CLOUD)
        
        # Process all pending hours
        success = processor.process_pending_hours(
            SERVICE_NAME, ENVIRONMENT_TYPE, PLAN_ID, MAX_HOURS_PER_RUN
        )
        
        if success:
            print("Metering processing completed successfully")
            sys.exit(0)
        else:
            print("Metering processing failed")
            sys.exit(1)
            
    except NoCredentialsError:
        print("Error: AWS credentials not found. Please configure AWS credentials.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()