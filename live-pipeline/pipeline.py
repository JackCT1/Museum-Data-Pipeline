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
        'group.id': 'zuckerberg-3',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': KAFKA_USERNAME,
        'sasl.password': KAFKA_PASSWORD,
        'session.timeout.ms': 6000,
        'heartbeat.interval.ms': 1000,
        'fetch.wait.max.ms': 6000,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': 'false',
        'max.poll.interval.ms': '86400000',
        'topic.metadata.refresh.interval.ms': "-1",
        "client.id": 'id-002-005',
    })
