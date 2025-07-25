#!/usr/bin/env python3
"""
Tests for MeteringProcessor class.
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch, mock_open

# Add the parent directory to the Python path to import metering_processor
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metering_processor import MeteringProcessor


class TestMeteringProcessor(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.state_file = os.path.join(self.temp_dir, "test_state.json")
        self.bucket_name = "test-bucket"
        self.clazar_api_url = "https://test-api.example.com/metering/"
        
        # Create processor instances
        self.processor = MeteringProcessor(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file,
            clazar_api_url=self.clazar_api_url,
            dry_run=False
        )
        
        self.dry_run_processor = MeteringProcessor(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file,
            clazar_api_url=self.clazar_api_url,
            dry_run=True
        )
    
    def tearDown(self):
        """Clean up test fixtures."""
        # Clean up temp files
        if os.path.exists(self.state_file):
            os.remove(self.state_file)
        os.rmdir(self.temp_dir)
    
    def test_init(self):
        """Test processor initialization."""
        self.assertEqual(self.processor.bucket_name, self.bucket_name)
        self.assertEqual(str(self.processor.state_file_path), self.state_file)
        self.assertEqual(self.processor.clazar_api_url, self.clazar_api_url)
        self.assertFalse(self.processor.dry_run)
        self.assertTrue(self.dry_run_processor.dry_run)
    
    def test_get_service_key(self):
        """Test service key generation."""
        key = self.processor.get_service_key("TestService", "PROD", "plan-123")
        self.assertEqual(key, "TestService:PROD:plan-123")
    
    def test_load_state_no_file(self):
        """Test loading state when file doesn't exist."""
        state = self.processor.load_state()
        self.assertEqual(state, {})
    
    def test_load_state_valid_file(self):
        """Test loading state from valid file."""
        test_state = {"service1:PROD:plan1": {"last_processed_hour": "2025-01-01T12:00:00Z"}}
        
        with open(self.state_file, 'w') as f:
            json.dump(test_state, f)
        
        state = self.processor.load_state()
        self.assertEqual(state, test_state)
    
    def test_load_state_invalid_json(self):
        """Test loading state from invalid JSON file."""
        with open(self.state_file, 'w') as f:
            f.write("invalid json")
        
        state = self.processor.load_state()
        self.assertEqual(state, {})
    
    def test_save_state(self):
        """Test saving state to file."""
        test_state = {"service1:PROD:plan1": {"last_processed_hour": "2025-01-01T12:00:00Z"}}
        
        self.processor.save_state(test_state)
        
        with open(self.state_file, 'r') as f:
            saved_state = json.load(f)
        
        self.assertEqual(saved_state, test_state)
    
    def test_get_last_processed_hour_no_record(self):
        """Test getting last processed hour when no record exists."""
        last_hour = self.processor.get_last_processed_hour("TestService", "PROD", "plan-123")
        self.assertIsNone(last_hour)
    
    def test_get_last_processed_hour_valid_record(self):
        """Test getting last processed hour with valid record."""
        test_time = datetime(2025, 1, 1, 12, 0, 0)
        test_state = {
            "TestService:PROD:plan-123": {
                "last_processed_hour": test_time.isoformat() + "Z"
            }
        }
        
        with open(self.state_file, 'w') as f:
            json.dump(test_state, f)
        
        last_hour = self.processor.get_last_processed_hour("TestService", "PROD", "plan-123")
        self.assertEqual(last_hour, test_time)
    
    def test_update_last_processed_hour(self):
        """Test updating last processed hour."""
        test_time = datetime(2025, 1, 1, 12, 0, 0)
        
        self.processor.update_last_processed_hour("TestService", "PROD", "plan-123", test_time)
        
        with open(self.state_file, 'r') as f:
            state = json.load(f)
        
        self.assertIn("TestService:PROD:plan-123", state)
        self.assertEqual(state["TestService:PROD:plan-123"]["last_processed_hour"], "2025-01-01T12:00:00Z")
    
    def test_get_next_hour_to_process_no_history(self):
        """Test getting next hour when no processing history exists."""
        with patch('metering_processor.datetime') as mock_datetime:
            mock_now = datetime(2025, 1, 2, 15, 30, 45)
            mock_datetime.utcnow.return_value = mock_now
            
            next_hour = self.processor.get_next_hour_to_process("TestService", "PROD", "plan-123")
            expected_hour = datetime(2025, 1, 1, 15, 0, 0)  # 24 hours ago, rounded to hour
            
            self.assertEqual(next_hour, expected_hour)
    
    def test_get_next_hour_to_process_with_history(self):
        """Test getting next hour with processing history."""
        last_processed = datetime(2025, 1, 1, 12, 0, 0)
        test_state = {
            "TestService:PROD:plan-123": {
                "last_processed_hour": last_processed.isoformat() + "Z"
            }
        }
        
        with open(self.state_file, 'w') as f:
            json.dump(test_state, f)
        
        # Mock the current time separately
        current_time = datetime(2025, 1, 2, 15, 0, 0)
        with patch.object(self.processor, 'get_last_processed_hour', return_value=last_processed):
            with patch('metering_processor.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value = current_time
                
                next_hour = self.processor.get_next_hour_to_process("TestService", "PROD", "plan-123")
                expected_hour = datetime(2025, 1, 1, 13, 0, 0)  # Next hour after last processed
                
                self.assertEqual(next_hour, expected_hour)
    
    def test_get_next_hour_to_process_caught_up(self):
        """Test getting next hour when caught up to current hour."""
        current_time = datetime(2025, 1, 2, 15, 0, 0)
        last_processed = current_time - timedelta(hours=1)  # One hour ago
        
        test_state = {
            "TestService:PROD:plan-123": {
                "last_processed_hour": last_processed.isoformat() + "Z"
            }
        }
        
        with open(self.state_file, 'w') as f:
            json.dump(test_state, f)
        
        # Mock the method calls separately to avoid datetime comparison issues
        with patch.object(self.processor, 'get_last_processed_hour', return_value=last_processed):
            with patch('metering_processor.datetime') as mock_datetime:
                mock_datetime.utcnow.return_value = current_time
                
                next_hour = self.processor.get_next_hour_to_process("TestService", "PROD", "plan-123")
                self.assertIsNone(next_hour)
    
    def test_get_hourly_s3_prefix(self):
        """Test S3 prefix generation."""
        target_hour = datetime(2025, 1, 15, 14, 0, 0)
        prefix = self.processor.get_hourly_s3_prefix("TestService", "PROD", "plan-123", target_hour)
        
        expected = "omnistrate-metering/TestService/PROD/plan-123/2025/01/15/14/"
        self.assertEqual(prefix, expected)
    
    @patch('metering_processor.boto3.client')
    def test_list_subscription_files_success(self, mock_boto3_client):
        """Test listing subscription files successfully."""
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Create a new processor instance to get the mocked S3 client
        processor = MeteringProcessor(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file,
            clazar_api_url=self.clazar_api_url,
            dry_run=False
        )
        
        mock_response = {
            'Contents': [
                {'Key': 'prefix/subscription1.json'},
                {'Key': 'prefix/subscription2.json'},
                {'Key': 'prefix/not_json.txt'},
            ]
        }
        mock_s3_client.list_objects_v2.return_value = mock_response
        
        files = processor.list_subscription_files("test-prefix/")
        
        self.assertEqual(len(files), 2)
        self.assertIn('prefix/subscription1.json', files)
        self.assertIn('prefix/subscription2.json', files)
        self.assertNotIn('prefix/not_json.txt', files)
    
    @patch('metering_processor.boto3.client')
    def test_list_subscription_files_no_contents(self, mock_boto3_client):
        """Test listing subscription files when no objects found."""
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Create a new processor instance to get the mocked S3 client
        processor = MeteringProcessor(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file,
            clazar_api_url=self.clazar_api_url,
            dry_run=False
        )
        
        mock_s3_client.list_objects_v2.return_value = {}
        
        files = processor.list_subscription_files("test-prefix/")
        self.assertEqual(files, [])
    
    @patch('metering_processor.boto3.client')
    def test_read_s3_json_file_success(self, mock_boto3_client):
        """Test reading S3 JSON file successfully."""
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Create a new processor instance to get the mocked S3 client
        processor = MeteringProcessor(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file,
            clazar_api_url=self.clazar_api_url,
            dry_run=False
        )
        
        test_data = [{"record1": "data1"}, {"record2": "data2"}]
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = json.dumps(test_data).encode('utf-8')
        mock_s3_client.get_object.return_value = mock_response
        
        data = processor.read_s3_json_file("test-key.json")
        self.assertEqual(data, test_data)
    
    @patch('metering_processor.boto3.client')
    def test_read_s3_json_file_invalid_json(self, mock_boto3_client):
        """Test reading S3 file with invalid JSON."""
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client
        
        # Create a new processor instance to get the mocked S3 client
        processor = MeteringProcessor(
            bucket_name=self.bucket_name,
            state_file_path=self.state_file,
            clazar_api_url=self.clazar_api_url,
            dry_run=False
        )
        
        mock_response = {
            'Body': Mock()
        }
        mock_response['Body'].read.return_value = b"invalid json"
        mock_s3_client.get_object.return_value = mock_response
        
        data = processor.read_s3_json_file("test-key.json")
        self.assertEqual(data, [])
    
    def test_aggregate_usage_data(self):
        """Test usage data aggregation."""
        usage_records = [
            {"externalPayerId": "payer1", "dimension": "dim1", "value": 10},
            {"externalPayerId": "payer1", "dimension": "dim1", "value": 5},
            {"externalPayerId": "payer2", "dimension": "dim1", "value": 20},
            {"externalPayerId": "payer1", "dimension": "dim2", "value": 15},
        ]
        
        aggregated = self.processor.aggregate_usage_data(usage_records)
        
        expected = {
            ("payer1", "dim1"): 15.0,
            ("payer2", "dim1"): 20.0,
            ("payer1", "dim2"): 15.0,
        }
        
        self.assertEqual(aggregated, expected)
    
    def test_aggregate_usage_data_missing_fields(self):
        """Test usage data aggregation with missing fields."""
        usage_records = [
            {"externalPayerId": "payer1", "dimension": "dim1", "value": 10},
            {"externalPayerId": "payer1", "value": 5},  # Missing dimension
            {"dimension": "dim1", "value": 20},  # Missing externalPayerId
        ]
        
        aggregated = self.processor.aggregate_usage_data(usage_records)
        
        expected = {
            ("payer1", "dim1"): 10.0,
        }
        
        self.assertEqual(aggregated, expected)
    
    @patch('metering_processor.requests.post')
    def test_send_to_clazar_success(self, mock_post):
        """Test sending data to Clazar successfully."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        aggregated_data = {
            ("payer1", "dim1"): 15.0,
            ("payer2", "dim1"): 20.0,
        }
        start_time = datetime(2025, 1, 1, 12, 0, 0)
        end_time = datetime(2025, 1, 1, 13, 0, 0)
        
        result = self.processor.send_to_clazar(aggregated_data, start_time, end_time)
        
        self.assertTrue(result)
        mock_post.assert_called_once()
    
    @patch('metering_processor.requests.post')
    def test_send_to_clazar_api_error(self, mock_post):
        """Test sending data to Clazar with API error."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_post.return_value = mock_response
        
        aggregated_data = {("payer1", "dim1"): 15.0}
        start_time = datetime(2025, 1, 1, 12, 0, 0)
        end_time = datetime(2025, 1, 1, 13, 0, 0)
        
        result = self.processor.send_to_clazar(aggregated_data, start_time, end_time)
        
        self.assertFalse(result)
    
    def test_send_to_clazar_dry_run(self):
        """Test sending data to Clazar in dry run mode."""
        aggregated_data = {
            ("payer1", "dim1"): 15.0,
            ("payer2", "dim1"): 20.0,
        }
        start_time = datetime(2025, 1, 1, 12, 0, 0)
        end_time = datetime(2025, 1, 1, 13, 0, 0)
        
        with patch('metering_processor.requests.post') as mock_post:
            result = self.dry_run_processor.send_to_clazar(aggregated_data, start_time, end_time)
            
            self.assertTrue(result)
            mock_post.assert_not_called()  # Should not make actual API call
    
    def test_send_to_clazar_empty_data(self):
        """Test sending empty data to Clazar."""
        aggregated_data = {}
        start_time = datetime(2025, 1, 1, 12, 0, 0)
        end_time = datetime(2025, 1, 1, 13, 0, 0)
        
        with patch('metering_processor.requests.post') as mock_post:
            result = self.processor.send_to_clazar(aggregated_data, start_time, end_time)
            
            self.assertTrue(result)
            mock_post.assert_not_called()
    
    @patch('metering_processor.MeteringProcessor.send_to_clazar')
    @patch('metering_processor.MeteringProcessor.aggregate_usage_data')
    @patch('metering_processor.MeteringProcessor.read_s3_json_file')
    @patch('metering_processor.MeteringProcessor.list_subscription_files')
    def test_process_hour_success(self, mock_list_files, mock_read_file, 
                                 mock_aggregate, mock_send):
        """Test processing an hour successfully."""
        target_hour = datetime(2025, 1, 1, 12, 0, 0)
        
        # Mock the method calls
        mock_list_files.return_value = ['file1.json', 'file2.json']
        mock_read_file.side_effect = [
            [{"record1": "data1"}],
            [{"record2": "data2"}]
        ]
        mock_aggregate.return_value = {("payer1", "dim1"): 15.0}
        mock_send.return_value = True
        
        result = self.processor.process_hour("TestService", "PROD", "plan-123", target_hour)
        
        self.assertTrue(result)
        mock_list_files.assert_called_once()
        self.assertEqual(mock_read_file.call_count, 2)
        mock_aggregate.assert_called_once()
        mock_send.assert_called_once()
    
    @patch('metering_processor.MeteringProcessor.list_subscription_files')
    def test_process_hour_no_files(self, mock_list_files):
        """Test processing an hour with no subscription files."""
        target_hour = datetime(2025, 1, 1, 12, 0, 0)
        mock_list_files.return_value = []
        
        result = self.processor.process_hour("TestService", "PROD", "plan-123", target_hour)
        
        self.assertTrue(result)
    
    @patch('metering_processor.MeteringProcessor.process_hour')
    @patch('metering_processor.MeteringProcessor.get_next_hour_to_process')
    def test_process_pending_hours_success(self, mock_get_next_hour, mock_process_hour):
        """Test processing pending hours successfully."""
        # Mock returning two hours to process, then None
        mock_get_next_hour.side_effect = [
            datetime(2025, 1, 1, 12, 0, 0),
            datetime(2025, 1, 1, 13, 0, 0),
            None
        ]
        mock_process_hour.return_value = True
        
        result = self.processor.process_pending_hours("TestService", "PROD", "plan-123", max_hours=5)
        
        self.assertTrue(result)
        self.assertEqual(mock_process_hour.call_count, 2)
    
    @patch('metering_processor.MeteringProcessor.process_hour')
    @patch('metering_processor.MeteringProcessor.get_next_hour_to_process')
    def test_process_pending_hours_failure(self, mock_get_next_hour, mock_process_hour):
        """Test processing pending hours with failure."""
        mock_get_next_hour.return_value = datetime(2025, 1, 1, 12, 0, 0)
        mock_process_hour.return_value = False
        
        result = self.processor.process_pending_hours("TestService", "PROD", "plan-123", max_hours=5)
        
        self.assertFalse(result)
        mock_process_hour.assert_called_once()


class TestMeteringProcessorIntegration(unittest.TestCase):
    """Integration tests that test multiple components together."""
    
    def setUp(self):
        """Set up integration test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.state_file = os.path.join(self.temp_dir, "integration_state.json")
        
        self.processor = MeteringProcessor(
            bucket_name="test-bucket",
            state_file_path=self.state_file,
            clazar_api_url="https://test-api.example.com/metering/",
            dry_run=True  # Use dry run for integration tests
        )
    
    def tearDown(self):
        """Clean up integration test fixtures."""
        if os.path.exists(self.state_file):
            os.remove(self.state_file)
        os.rmdir(self.temp_dir)
    
    def test_state_persistence_workflow(self):
        """Test the complete state persistence workflow."""
        service_name = "TestService"
        environment_type = "PROD"
        plan_id = "plan-123"
        
        # Initially no last processed hour
        last_hour = self.processor.get_last_processed_hour(service_name, environment_type, plan_id)
        self.assertIsNone(last_hour)
        
        # Update the last processed hour
        test_hour = datetime(2025, 1, 1, 12, 0, 0)
        self.processor.update_last_processed_hour(service_name, environment_type, plan_id, test_hour)
        
        # Verify it was saved
        last_hour = self.processor.get_last_processed_hour(service_name, environment_type, plan_id)
        self.assertEqual(last_hour, test_hour)
        
        # Create a new processor instance and verify state persists
        new_processor = MeteringProcessor(
            bucket_name="test-bucket",
            state_file_path=self.state_file,
            clazar_api_url="https://test-api.example.com/metering/",
            dry_run=True
        )
        
        last_hour = new_processor.get_last_processed_hour(service_name, environment_type, plan_id)
        self.assertEqual(last_hour, test_hour)


if __name__ == '__main__':
    unittest.main()
