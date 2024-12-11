import unittest
import os
import json
import shutil
from main import SalesDataJob, init_firebase

class TestSalesDataJob(unittest.TestCase):
    def setUp(self):
        # Initialize real Firestore
        self.db = init_firebase()
        self.raw_dir = './path/to/my_dir/raw'
        self.job = SalesDataJob(self.db, self.raw_dir)

    def tearDown(self):
        if os.path.exists(self.raw_dir):
            shutil.rmtree(self.raw_dir)

    def test_fetch_and_save_real_data(self):
        """Test fetching data from the real Firestore and saving it to a file."""
        # Set up test parameters
        date = "2024-12-09"
        feature = "goldAM"

        # Execute the job
        self.job.execute(date, feature)

        # Check if the file was created
        file_path = os.path.join(self.raw_dir, feature, f"{date}.json")
        self.assertTrue(os.path.exists(file_path))

        # Check the content of the file
        with open(file_path, 'r') as file:
            saved_data = json.load(file)

        # Verify that the file contains data
        self.assertTrue(len(saved_data) > 0)
        for record in saved_data:
            self.assertIn("date", record)
            self.assertIn(feature, record)

        # Print the contents of the saved file
        print("Contents of the saved file:")
        print(saved_data)

if __name__ == '__main__':
    unittest.main()
