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

if __name__ == "__main__":
    ''