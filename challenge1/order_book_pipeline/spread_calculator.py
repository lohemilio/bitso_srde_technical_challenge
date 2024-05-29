# challenge1/order_book_pipeline/spread_calculator.py
def calculate_spread(order_book):
    """Calculate the bid-ask spread.
    
    Args:
        order_book (dict): The order book data.
    
    Returns:
        dict: Calculated spread and associated data.
    """
    timestamp = order_book['payload']['updated_at']
    bids = order_book['payload']['bids']
    asks = order_book['payload']['asks']
    
    if not bids or not asks:
        raise ValueError("No bids or asks in the order book.")
    
    best_bid = float(bids[0]['price'])
    best_ask = float(asks[0]['price'])
    spread = (best_ask - best_bid) * 100 / best_ask
    
    return {
        'orderbook_timestamp': timestamp,
        'bid': best_bid,
        'ask': best_ask,
        'spread': spread
    }
