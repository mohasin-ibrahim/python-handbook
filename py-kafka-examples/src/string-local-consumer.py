from confluent_kafka import Consumer
import logging

input_topics = ["my-topic1"]

config = { 'bootstrap.servers': 'localhost:9092',
            'group.id': 'phoenix-local-consumer',
            'auto.offset.reset': 'earliest' }

logging.getLogger().setLevel(logging.INFO)

consumer = Consumer(config)

consumer.subscribe(input_topics)
logging.info("Subscribed to the topic : {}".format(input_topics))

def handle_msg(msg):
    logging.info("The record was read from the kafka topic {}, partition {}, offset {}".format(msg.topic(), msg.partition(), msg.offset()))
    logging.info("The record Keys is {}, Value is {}".format(msg.key(), msg.value()))
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
    

