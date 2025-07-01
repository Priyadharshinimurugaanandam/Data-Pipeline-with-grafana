from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
from pymongo import MongoClient
import os

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="push_csv_from_project_to_mongo",
    default_args=default_args,
    description="Push CSV from local Astro project folder to MongoDB",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *",  # every hour
    catchup=False,
    tags=["csv", "mongo", "local"]
)
def pipeline():

    @task()
    def push_csv_to_mongo():
        file_path = "/usr/local/airflow/data/joints.csv"

        print("📂 Looking for file at:", file_path)
        print("📁 Contents of data folder:", os.listdir("/usr/local/airflow/data"))

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"❌ File not found: {file_path}")

        df = pd.read_csv(file_path)
        print(f"📊 CSV Loaded. Total rows: {len(df)}")

        records = df.to_dict(orient="records")

        # Connect to MongoDB (running on your host machine)
        client = MongoClient("mongodb://host.docker.internal:27017/")
        db = client["airflow_db"]
        collection = db["joints_collection"]

        # Optional: clear collection before inserting
        # collection.delete_many({})

        collection.insert_many(records)
        print("✅ Data successfully inserted into MongoDB.")

    push_csv_to_mongo()

pipeline()