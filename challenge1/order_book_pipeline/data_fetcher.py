# challenge1/order_book_pipeline/data_fetcher.py
import requests
from .config import BITSO_API_URL

def fetch_order_book(book):
    """Fetch order book data from Bitso API.
    
    Args:
        book (str): The book to fetch data for (e.g., 'btc_mxn').
    
    Returns:
        dict: Parsed JSON response from the API.
    """
    response = requests.get(BITSO_API_URL.format(book))
    response.raise_for_status()
    return response.json()
