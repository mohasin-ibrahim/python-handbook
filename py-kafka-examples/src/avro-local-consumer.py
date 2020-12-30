from confluent_kafka import Consumer
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
import logging

input_topics = ["avro-topic"]

config = { 'bootstrap.servers': 'localhost:9092',
            'group.id': 'phoenix-local-consumer-2',
            'auto.offset.reset': 'earliest' }

sr_config = {  'url': 'http://localhost:8081',
               'auto.register.schemas': False }


sr_client = CachedSchemaRegistryClient(sr_config)

serializer = MessageSerializer(sr_client)

logging.getLogger().setLevel(logging.INFO)

consumer = Consumer(config)

consumer.subscribe(input_topics)
logging.info("Subscribed to the topic : {}".format(input_topics))

def handle_msg(msg):
    value = serializer.decode_message(msg.value())
    key = msg.key().decode("utf-8")
    logging.info("The record was read from the kafka topic {}, partition {}, offset {}".format(msg.topic(), msg.partition(), msg.offset()))
    logging.info("The record Keys is {}, Value is {}".format(key, value))
    consumer.commit(msg)


while True:
    msg = consumer.poll(1)

    if msg is None: continue

    if msg.error():
        if (msg.error().fatal()):
            logging.error("Error -- " + msg.error().str())
        else:
            logging.warning("Warning -- " + msg.error().str())
            continue
    
    handle_msg(msg)

consumer.close()
    

