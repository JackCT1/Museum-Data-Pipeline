from datetime import datetime
import logging
import os

from confluent_kafka import Consumer, TopicPartition, OFFSET_END
from dotenv import load_dotenv
from psycopg2 import connect
from psycopg2.extensions import connection
