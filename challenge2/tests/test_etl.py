import unittest
import pandas as pd
from etl_pipeline.etl import transform_data, save_data
import os

class TestETL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create sample dataframes for testing
        cls.deposit_data = pd.DataFrame({
            'id': [1, 2, 3],
            'event_timestamp': ['2023-05-01 00:00:00', '2023-05-01 01:00:00', '2023-05-01 02:00:00'],
            'user_id': ['user_1', 'user_2', 'user_3'],
            'amount': [100.0, 200.0, 300.0],
            'currency': ['USD', 'EUR', 'JPY'],
            'tx_status': ['complete', 'complete', 'complete']
        })

        cls.event_data = pd.DataFrame({
            'id': [1, 2, 3],
            'event_timestamp': ['2023-05-01 00:00:00', '2023-05-01 01:00:00', '2023-05-01 02:00:00'],
            'user_id': ['user_1', 'user_2', 'user_3'],
            'event_name': ['login', 'logout', 'login']
        })

        cls.user_id_data = pd.DataFrame({
            'user_id': ['user_1', 'user_2', 'user_3']
        })

        cls.withdrawals_data = pd.DataFrame({
            'id': [1, 2, 3],
            'event_timestamp': ['2023-05-01 00:00:00', '2023-05-01 01:00:00', '2023-05-01 02:00:00'],
            'user_id': ['user_1', 'user_2', 'user_3'],
            'amount': [50.0, 150.0, 250.0],
            'currency': ['USD', 'EUR', 'JPY'],
            'tx_status': ['complete', 'complete', 'complete']
        })

    def test_transform_data(self):
        # Test the transform_data function
        transactions_data, event_data, user_id_data = transform_data(
            self.deposit_data, self.event_data, self.user_id_data, self.withdrawals_data
        )

        self.assertTrue(pd.api.types.is_datetime64_any_dtype(transactions_data['event_timestamp']))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(event_data['event_timestamp']))

        self.assertIn('transaction_type', transactions_data.columns)
        self.assertEqual(set(transactions_data['transaction_type']), {'deposit', 'withdrawal'})

    def test_save_data(self):
        # Test the save_data function
        output_dir = 'output_tables'
        os.makedirs(output_dir, exist_ok=True)

        transactions_data, event_data, user_id_data = transform_data(
            self.deposit_data, self.event_data, self.user_id_data, self.withdrawals_data
        )

        save_data(transactions_data, event_data, user_id_data)

        transactions_file = os.path.join(output_dir, 'transactions.csv')
        events_file = os.path.join(output_dir, 'events.csv')
        users_file = os.path.join(output_dir, 'users.csv')

        print(f"Checking for file existence:\n - {transactions_file}\n - {events_file}\n - {users_file}")

        self.assertTrue(os.path.exists(transactions_file))
        self.assertTrue(os.path.exists(events_file))
        self.assertTrue(os.path.exists(users_file))

        # Cleanup
        os.remove(transactions_file)
        os.remove(events_file)
        os.remove(users_file)
        os.rmdir(output_dir)

if __name__ == '__main__':
    unittest.main()
