import pandas as pd
import os

def load_data():
    # Paths to the CSV files
    deposit_file_path = 'data/deposit_sample_data.csv'
    event_file_path = 'data/event_sample_data.csv'
    user_id_file_path = 'data/user_id_sample_data.csv'
    withdrawals_file_path = 'data/withdrawals_sample_data.csv'

    # Load the CSV files into DataFrames
    deposit_data = pd.read_csv(deposit_file_path)
    event_data = pd.read_csv(event_file_path)
    user_id_data = pd.read_csv(user_id_file_path)
    withdrawals_data = pd.read_csv(withdrawals_file_path)

    return deposit_data, event_data, user_id_data, withdrawals_data

def transform_data(deposit_data, event_data, user_id_data, withdrawals_data):
    # Convert event_timestamp to datetime with format inference
    deposit_data['event_timestamp'] = pd.to_datetime(deposit_data['event_timestamp'], errors='coerce', utc=True)
    event_data['event_timestamp'] = pd.to_datetime(event_data['event_timestamp'], errors='coerce', utc=True)
    withdrawals_data['event_timestamp'] = pd.to_datetime(withdrawals_data['event_timestamp'], errors='coerce', utc=True)

    # Add transaction_type to distinguish between deposits and withdrawals
    deposit_data['transaction_type'] = 'deposit'
    withdrawals_data['transaction_type'] = 'withdrawal'

    # Rename columns to be consistent
    deposit_data = deposit_data.rename(columns={'id': 'transaction_id'})
    withdrawals_data = withdrawals_data.rename(columns={'id': 'transaction_id'})

    # Combine deposits and withdrawals into a single transactions table
    transactions_data = pd.concat([deposit_data, withdrawals_data], ignore_index=True)

    return transactions_data, event_data, user_id_data

def save_data(transactions_data, event_data, user_id_data):
    # Save transformed tables to new CSV files
    output_dir = 'output_tables'
    os.makedirs(output_dir, exist_ok=True)

    transactions_data.to_csv(os.path.join(output_dir, 'transactions.csv'), index=False)
    event_data.to_csv(os.path.join(output_dir, 'events.csv'), index=False)
    user_id_data.to_csv(os.path.join(output_dir, 'users.csv'), index=False)

def main():
    deposit_data, event_data, user_id_data, withdrawals_data = load_data()
    transactions_data, event_data, user_id_data = transform_data(deposit_data, event_data, user_id_data, withdrawals_data)
    save_data(transactions_data, event_data, user_id_data)

if __name__ == "__main__":
    main()
