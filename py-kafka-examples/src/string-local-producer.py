from confluent_kafka import Producer
import logging
import time

config = { 'bootstrap.servers': 'localhost:9092',
            'client.id': 'phoenix-local-producer' }


logging.getLogger().setLevel(logging.INFO)

def delivery_report(err, msg):
    if err is not None:
        logging.error("Delivery failed for User record {}: {}".format(msg.key(), err))
    else:
        logging.info("Record successfully delivered -> TOPIC: {} - PARTITION: [{}] - OFFSET {}".format(msg.topic(), msg.partition(), msg.offset()))

producer = Producer(config)

for i in range(50):
    producer.produce("my-topic1", key=str(i), value="value-"+str(i), on_delivery=(delivery_report))
    time.sleep(3)

# EOS - Synchronous writes
producer.flush()

    
