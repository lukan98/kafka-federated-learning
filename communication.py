import json
import numpy as np
from constants import *
from confluent_kafka import Consumer, Producer


def make_producer(server):
    return Producer({BOOTSTRAP_SERVER_KEY: server})


def make_consumer(server, consumer_group_id, topic):
    consumer = Consumer(
        {
            BOOTSTRAP_SERVER_KEY: server,
            GROUP_ID_KEY: consumer_group_id,
            OFFSET_RESET_KEY: 'earliest'
        }
    )
    consumer.subscribe([topic])
    return consumer


def serialize_message(message):
    return json.dumps(message)


def deserialize_message(message):
    return json.loads(message)


class Communicator:

    def __init__(self, server, consumer_group_id, input_topic, output_topic, polling_timeout):
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.consumer = make_consumer(server, consumer_group_id, input_topic)
        self.producer = make_producer(server)
        self.polling_timeout = polling_timeout

    def produce(self, message):
        def callback(err, msg):
            if err is not None:
                print("Failed to deliver message: %s: %s" % (str(msg), str(err)))

        self.producer.produce(self.output_topic, serialize_message(message), callback=callback)
        self.producer.poll(self.polling_timeout)

    def consume(self, number_of_messages):
        messages = []
        for i in range(number_of_messages):
            while True:
                message = self.consumer.poll(self.polling_timeout)

                if message is None:
                    continue
                if message.error():
                    print("Consumer error: {}".format(message.error()))
                    continue

                messages.append(deserialize_message(message.value().decode('utf-8')))
                break
        return messages
