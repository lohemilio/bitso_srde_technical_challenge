# challenge1/main.py
import time
from datetime import datetime
import logging
from order_book_pipeline.data_fetcher import fetch_order_book
from order_book_pipeline.spread_calculator import calculate_spread
from order_book_pipeline.data_storage import store_data, create_partition_directory
from order_book_pipeline.config import BOOKS, FETCH_INTERVAL, STORAGE_INTERVAL
from order_book_pipeline.utils import get_end_time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ALERT_THRESHOLDS = [1.0, 0.5, 0.1]  # in percentage

def check_alerts(spread):
    """Check if the spread exceeds any of the alert thresholds.
    
    Args:
        spread (float): The calculated spread.
    """
    for threshold in ALERT_THRESHOLDS:
        if spread > threshold:
            logging.warning(f"Alert: Spread {spread:.3f}% exceeds threshold of {threshold}%")

def main():
    start_time = datetime.utcnow()
    end_time = get_end_time(start_time, STORAGE_INTERVAL)
    records = []

    logging.info("Starting data collection...")

    while datetime.utcnow() < end_time:
        logging.info(f"New loop iteration at {datetime.utcnow()}")
        for book in BOOKS:
            try:
                order_book = fetch_order_book(book)
                logging.info(f"Fetched order book for {book}")

                spread_data = calculate_spread(order_book)
                logging.info(f"Calculated spread for {book}")

                spread_data['book'] = book
                records.append(spread_data)
                logging.info(f"Appended spread data for {book}")

                # Check for alerts
                check_alerts(spread_data['spread'])
            except Exception as e:
                logging.error(f"Error processing book {book}: {e}")

        time.sleep(FETCH_INTERVAL)
        logging.info(f"Sleeping for {FETCH_INTERVAL} seconds")

    partition_dir = create_partition_directory()
    store_data(records, partition_dir)
    logging.info(f"Data stored in {partition_dir}")

if __name__ == "__main__":
    main()
