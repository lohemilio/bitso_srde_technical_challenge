# challenge1/order_book_pipeline/utils.py
from datetime import datetime, timedelta

def get_end_time(start_time, interval_seconds):
    """Calculate the end time based on start time and interval.
    
    Args:
        start_time (datetime): The start time.
        interval_seconds (int): The interval in seconds.
    
    Returns:
        datetime: The calculated end time.
    """
    return start_time + timedelta(seconds=interval_seconds)
