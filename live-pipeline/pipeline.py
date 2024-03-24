from datetime import datetime
import logging
import os

from confluent_kafka import Consumer, TopicPartition, OFFSET_END
from dotenv import load_dotenv
from psycopg2 import connect
from psycopg2.extensions import connection

logging.getLogger().setLevel(logging.INFO)

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
KAFKA_SERVER = os.getenv('KAFKA_SERVER')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
GROUP = os.getenv('GROUP')

def get_logger(log_level: str) -> logging.Logger:
    """
    Returns:
    - formatted logger
    """
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s: %(levelname)s: %(message)s'
    )
    logger = logging.getLogger()                    
    return logger

def start_consumer() -> Consumer:
    """
    Connects consumer to Kafka topic

    Returns:
    - consumer connection
    """
    return Consumer({
        'bootstrap.servers': KAFKA_SERVER,
        'group.id': GROUP,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_USERNAME,
        'sasl.password': KAFKA_PASSWORD,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': 'false',
    })

def insert_message_row(conn: connection, message: dict) -> bool:
    """
    Uploads message to relevant table in database
    Returns True if message successfully uploaded, False otherwise
    """

    at = message.get("at")
    site = message.get("site")
    val = message.get("val")
    type = message.get("type")

    if type is not None:
        with conn.cursor() as cur:
            cur.execute(f"""
                        INSERT INTO support_events (exhibit_id, support_type_id, at)
                        VALUES ({site}, {type}, '{at}');
                        """)
            conn.commit()
            return True

    elif type is None:
        with conn.cursor() as cur:
            cur.execute(f"""
                        INSERT INTO rating_events (exhibit_id, rating_value_id, at)
                        VALUES ({site}, {val}, '{at}');
                        """)
            conn.commit()
            return True

    return False
