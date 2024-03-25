from ast import literal_eval
from datetime import datetime
import logging
import os

from confluent_kafka import Consumer, TopicPartition, OFFSET_END, KafkaException
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
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
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

def format_event_row(message: dict) -> dict:
    """
    Formats Kafka message for database insertion
    """
    
    at = datetime.fromisoformat(message.get("at"))
    at = datetime(at.year, at.month, at.day, at.hour, at.minute, at.second)

    site = message.get("site")
    val = message.get("val")
    type = message.get("type")

    site += 1

    if val != -1:
        val += 1

    if type is not None:
        type += 1
    
    return message

def insert_event_row(conn: connection, message: dict) -> bool:
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

def upload_event_to_database(consumer: str, topic: str, conn: connection) -> None:

    c = start_consumer()
    c.subscribe([KAFKA_TOPIC])
    message = consumer.poll(1)
    while True:
        if message:
            try:
                message_dictionary = literal_eval(message.value().decode())
                message = format_event_row(message_dictionary)
                insert_event_row(conn, message)
                logging.info("Message successfully uploaded to database")
            except KafkaException as e:
                logging.error(f"Error raised whilst accessing Kafka stream: {e}")
            finally:
                c.close()

if __name__ == '__main__':

    log = get_logger(logging.INFO)

    consumer = start_consumer()

    tp = TopicPartition(KAFKA_TOPIC, 0)
    tp.offset = OFFSET_END
    consumer.assign([tp])

    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    db_connection = engine.connect()
    upload_event_to_database(consumer, KAFKA_TOPIC, db_connection)