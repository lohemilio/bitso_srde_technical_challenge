# challenge1/order_book_pipeline/data_storage.py
import os
import csv
from datetime import datetime
from .config import PARTITION_BASE_DIR

def store_data(records, partition_dir):
    """Store data in CSV format.
    
    Args:
        records (list): List of records to store.
        partition_dir (str): Directory where the CSV will be stored.
    """
    if not os.path.exists(partition_dir):
        os.makedirs(partition_dir)
    
    file_path = os.path.join(partition_dir, 'order_book_data.csv')
    with open(file_path, 'w', newline='') as csvfile:
        fieldnames = ['orderbook_timestamp', 'book', 'bid', 'ask', 'spread']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)

def create_partition_directory():
    """Create a directory structure similar to S3 partitions.
    
    Returns:
        str: Path to the partition directory.
    """
    now = datetime.utcnow()
    partition_dir = os.path.join(PARTITION_BASE_DIR, 
                                 f'year={now.year}', 
                                 f'month={now.month}', 
                                 f'day={now.day}', 
                                 f'hour={now.hour}', 
                                 f'minute={now.minute}')
    return partition_dir
