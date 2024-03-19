import logging
import os

from dotenv import load_dotenv
import pandas as pd
from boto3 import client
import botocore.exceptions
import botocore.exceptions
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()

BUCKET_NAME = os.getenv()
ACCESS_KEY_ID = os.getenv()
SECRET_ACCESS_KEY = os.getenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
connection = engine.connect()
session = sessionmaker(bind=engine)
s3 = client('s3', aws_access_key_id = ACCESS_KEY_ID, aws_secret_access_key = SECRET_ACCESS_KEY)

def download_relevant_files_from_s3() -> bool:
    contents = s3.list_objects(Bucket=BUCKET_NAME)["Contents"]
    file_names = [list_object["Key"] for list_object in contents]
    for file in file_names:
        s3.download_file(BUCKET_NAME, file, f"downloads/{file}")
    logging.info("All files successfully downloaded from S3")
    return True

def load_files_into_pandas_dataframes() -> pd.DataFrame:
    events_df = pd.read_csv('downloads/lmnh_hist_data_all.csv')
    ratings_df = events_df[events_df['val'] != -1]
    support_df = events_df[events_df['val'] == -1]
    ratings_df = ratings_df[['site', 'val', 'at']]
    support_df = support_df[['site', 'type', 'at']]
    logging.info("Files successfully loaded into Pandas Dataframes")
    return ratings_df, support_df

def format_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe[dataframe.columns[0]] += 1
    dataframe[dataframe.columns[1]] += 1
    dataframe[dataframe.columns[2]].to_timestamp
    return dataframe

if __name__ == "__main__":
    ''