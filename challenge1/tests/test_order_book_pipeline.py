# challenge1/tests/test_order_book_pipeline.py
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
from order_book_pipeline.spread_calculator import calculate_spread
from order_book_pipeline.data_storage import create_partition_directory
from order_book_pipeline.utils import get_end_time
from datetime import datetime, timedelta

class TestDataEngineeringPipeline(unittest.TestCase):
    
    def test_calculate_spread(self):
        order_book = {
            'payload': {
                'updated_at': '2023-05-25T00:00:00+00:00',
                'bids': [{'price': '100.0'}],
                'asks': [{'price': '110.0'}]
            }
        }
        spread_data = calculate_spread(order_book)
        self.assertEqual(spread_data['spread'], 9.090909090909092)

    def test_create_partition_directory(self):
        partition_dir = create_partition_directory()
        self.assertTrue(partition_dir.startswith('data_lake'))

    def test_get_end_time(self):
        start_time = datetime.utcnow()
        interval_seconds = 600
        end_time = get_end_time(start_time, interval_seconds)
        self.assertEqual(end_time, start_time + timedelta(seconds=interval_seconds))

if __name__ == "__main__":
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
