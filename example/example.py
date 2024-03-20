from confluent_kafka import Producer
from dotenv import load_dotenv
import random
import os
import time
import boto3

load_dotenv()

S3_BUCKET_NAME = os.getenv('MY_BUCKET_NAME')

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_purchase_event():
    products = ["Laptop", "Phone", "Tablet", "Smartwatch"]
    users = ["Alice", "Bob", "Charlie", "David", "Eve"]

    product = random.choice(products)
    user = random.choice(users)
    amount = round(random.uniform(10, 1000), 2)  # Random amount between $10 and $1000
    timestamp = int(time.time() * 1000)  # Current timestamp in milliseconds

    return f"{user},{product},{amount},{timestamp}"

def write_to_s3(data, bucket_name, key):
    s3 = boto3.client('s3')
    s3.put_object(Body=data, Bucket=bucket_name, Key=key)

def main():
    conf = {'bootstrap.servers': "localhost:9092"}

    producer = Producer(conf)

    topic = "purchase-events"

    s3_bucket_name = S3_BUCKET_NAME
    s3_key = "purchase-events-data.csv"

    while True:
        event_data = generate_purchase_event()
        producer.produce(topic, value=event_data, callback=delivery_report)
        producer.poll(0)  # Trigger delivery report callback
        
        # Write data to S3
        write_to_s3(event_data.encode(), s3_bucket_name, s3_key)
        
        time.sleep(random.uniform(1, 5))  # Random sleep between 1 and 5 seconds

    producer.flush()

if __name__ == "__main__":
    main()
