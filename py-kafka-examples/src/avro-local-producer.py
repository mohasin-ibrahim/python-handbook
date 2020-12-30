from confluent_kafka import Producer
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
import logging
import time
import json
import os

config = { 'bootstrap.servers': 'localhost:9092',
           'client.id': 'phoenix-local-producer' }

sr_config = {  'url': 'http://localhost:8081',
               'auto.register.schemas': False }

topic = 'avro-topic-1'
suffix = '-value'
subject = topic+suffix

sr_client = CachedSchemaRegistryClient(sr_config)
schema_details = sr_client.get_latest_schema(subject)
RECORD_SCHEMA = schema_details[1]

serializer = MessageSerializer(sr_client)

class AvroModel:
    def __init__(self, id, name):
        self.id = id
        self.name = name

data = AvroModel(None, None)

logging.getLogger().setLevel(logging.INFO)

def delivery_report(err, msg):
    if err is not None:
        logging.error("Delivery failed for User record {}: {}".format(msg.key(), err))
    else:
        logging.info("Record successfully delivered -> TOPIC: {} - PARTITION: [{}] - OFFSET {}".format(msg.topic(), msg.partition(), msg.offset()))

producer = Producer(config)

# Avro data needs to be loaded as an encoded dict with schema and the Producer takes care of inserting it properly
# Just load 5 records into the source topic
for i in range(5):
    data.id = i
    data.name = "Value of the string that was inserted is ~~~~~~ ========= -----> "+str(i)
    data_dict = data.__dict__
    avro_record = serializer.encode_record_with_schema(topic="avro-topic-1", schema=RECORD_SCHEMA, record=data_dict, is_key=False)
    producer.produce("avro-topic", key=str(1), value=avro_record, on_delivery=(delivery_report))
    time.sleep(3)

# EOS - Synchronous writes
producer.flush()