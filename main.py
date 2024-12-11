import os
import json
import shutil
from flask import Flask, request, jsonify
from firebase_admin import credentials, firestore, initialize_app

# Глобальна змінна для Firebase
firebase_app = None

def init_firebase():
    global firebase_app
    if not firebase_app:
        cred = credentials.Certificate('laba3.json')
        firebase_app = initialize_app(cred)
    return firestore.client()

db = init_firebase()

app = Flask(__name__)

@app.route('/fetch_data', methods=['POST'])
def fetch_data():
    try:
        data = request.json
        date = data['date']
        feature = data['feature']
        raw_dir = data.get('raw_dir', './path/to/my_dir/raw')

        job = SalesDataJob(db, raw_dir)
        job.execute(date, feature)

        return jsonify({"message": "Data fetched successfully."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

class SalesDataJob:
    def __init__(self, db, raw_dir):
        self.db = db
        self.raw_dir = raw_dir

    def execute(self, date, feature):
        self._clean_raw_dir()
        sales_data = self._fetch_sales_data(date, feature)
        self._save_sales_data(date, feature, sales_data)

    def _clean_raw_dir(self):
        if os.path.exists(self.raw_dir):
            shutil.rmtree(self.raw_dir)
        os.makedirs(self.raw_dir)

    def _fetch_sales_data(self, date, feature):
        sales_data = self.db.collection('Ingeneria5').where('Date', '==', date).get()
        result = []
        for sale in sales_data:
            sale_dict = sale.to_dict()
            if feature in sale_dict and sale_dict[feature] != 0.0:  # Исключаем записи с нулевым значением
                result.append({"date": sale_dict["Date"], feature: sale_dict[feature]})
        return result

    def _save_sales_data(self, date, feature, sales_data):
        dir_path = os.path.join(self.raw_dir, feature)
        os.makedirs(dir_path, exist_ok=True)

        file_path = os.path.join(dir_path, f"{date}.json")
        with open(file_path, 'w') as file:
            json.dump(sales_data, file, indent=4)

if __name__ == '__main__':
    app.run(port=8081)