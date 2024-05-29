# order_book_pipeline/config.py
BITSO_API_URL = "https://api.bitso.com/v3/order_book/?book={}"
BOOKS = ["btc_mxn", "usd_mxn"]
PARTITION_BASE_DIR = 'data_lake'
FETCH_INTERVAL = 1  # seconds
STORAGE_INTERVAL = 600  # seconds (10 minutes)
