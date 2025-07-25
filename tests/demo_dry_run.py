#!/usr/bin/env python3
"""
Demo script to show dry run functionality with mock data.
"""

import json
import os
import sys
import tempfile
from datetime import datetime
from unittest.mock import patch

# Add the parent directory to the Python path so we can import metering_processor
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metering_processor import MeteringProcessor


def demo_dry_run():
    """Demonstrate dry run functionality with mock data."""
    
    # Create a temporary state file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        state_file = f.name
    
    # Initialize processor in dry run mode
    processor = MeteringProcessor(
        bucket_name="demo-bucket",
        state_file_path=state_file,
        clazar_api_url="https://api.clazar.io/metering/",
        dry_run=True
    )
    
    print("=== Dry Run Demo ===")
    print("This demo shows how the dry run mode works with mock data.")
    print()
    
    # Mock S3 operations to return sample data
    sample_usage_data = [
        {"externalPayerId": "customer-123", "dimension": "cpu_core_hours", "value": 10},
        {"externalPayerId": "customer-123", "dimension": "cpu_core_hours", "value": 5},
        {"externalPayerId": "customer-456", "dimension": "memory_byte_hours", "value": 100},
        {"externalPayerId": "customer-123", "dimension": "memory_byte_hours", "value": 50},
        {"externalPayerId": "customer-123", "dimension": "storage_allocated_byte_hours", "value": 200},
        {"externalPayerId": "customer-123", "dimension": "storage_allocated_byte_hours", "value": 100}
    ]
    
    with patch.object(processor, 'list_subscription_files') as mock_list_files, \
         patch.object(processor, 'read_s3_json_file') as mock_read_file:
        
        # Mock S3 file listing
        mock_list_files.return_value = ['subscription1.json', 'subscription2.json']
        
        # Mock S3 file reading
        mock_read_file.side_effect = [
            sample_usage_data[:2],  # First file
            sample_usage_data[2:]   # Second file
        ]
        
        # Process one hour
        target_hour = datetime(2025, 1, 15, 14, 0, 0)
        print(f"Processing hour: {target_hour}")
        print(f"Sample input data: {json.dumps(sample_usage_data, indent=2)}")
        print()
        
        success = processor.process_hour("DemoService", "PROD", "demo-plan", target_hour)
        
        print()
        print(f"Processing successful: {success}")
        print()
        print("Notice how in dry run mode:")
        print("- All data processing happens normally")
        print("- The payload is logged with full details")
        print("- No actual HTTP request is made")
        print("- The function still returns success")


if __name__ == "__main__":
    demo_dry_run()
