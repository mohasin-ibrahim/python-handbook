from confluent_kafka import Producer
import logging
import time
import json
import os

config = { 'bootstrap.servers': 'localhost:9092',
           'client.id': 'phoenix-local-producer' }

filename = os.getcwd()+"/files/nyphil.json"

# Load the file as JSON object
with open(filename) as f:
    data = json.load(f)

logging.getLogger().setLevel(logging.INFO)

def delivery_report(err, msg):
    if err is not None:
        logging.error("Delivery failed for User record {}: {}".format(msg.key(), err))
    else:
        logging.info("Record successfully delivered -> TOPIC: {} - PARTITION: [{}] - OFFSET {}".format(msg.topic(), msg.partition(), msg.offset()))

producer = Producer(config)

# JSON data needs to be loaded as string of JSON and the Producer takes care of inserting it properly
# Just load first 3 records from the JSON array
for i in range(len(data['programs'])):
    if i < 3:
        # Convert the JSON to string of JSON
        data_json = json.dumps(data['programs'][i])
        producer.produce("my-topic1", key=str(i), value=data_json, on_delivery=(delivery_report))
        time.sleep(3)

# EOS - Synchronous writes
producer.flush()