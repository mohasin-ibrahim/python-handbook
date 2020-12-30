from confluent_kafka import Consumer
import logging
import json

input_topics = ["my-topic1"]

config = { 'bootstrap.servers': 'localhost:9092',
            'group.id': 'phoenix-local-consumer-3',
            'auto.offset.reset': 'earliest' }

logging.getLogger().setLevel(logging.INFO)

consumer = Consumer(config)

consumer.subscribe(input_topics)
logging.info("Subscribed to the topic : {}".format(input_topics))

def handle_msg(msg):
    value = msg.value().decode("utf-8")
    json_object = json.loads(value)
    id = json_object['id']
    logging.info("The record was read from the kafka topic {}, partition {}, offset {}".format(msg.topic(), msg.partition(), msg.offset()))
    for work in json_object['works']:
        for soloist in work['soloists']:
            soloistName = soloist['soloistName']
            soloistInstrument = soloist['soloistInstrument']
            logging.info("The Id is {}, SoloistName - {}, SoloistInstrument - {}".format(id, soloistName, soloistInstrument))
    
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
    

