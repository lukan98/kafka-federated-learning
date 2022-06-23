import json
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


class SampleProducer:

    def __init__(self, server, polling_timeout):
        self.polling_timeout = polling_timeout
        self.producer = make_producer(server)

    def produce(self, message, topic_name):
        def callback(err, msg):
            if err is not None:
                print(
                    "Failed to deliver message:"
                    "%s: %s" % (str(msg), str(err)))

        self.producer.produce(
            topic_name,
            json.dumps(message),
            callback=callback)
        self.producer.poll(self.polling_timeout)


class SampleConsumer:

    def __init__(self, server, consumer_group_id, topic_name, polling_timeout):
        self.consumer = make_consumer(server, consumer_group_id, topic_name)
        self.polling_timeout = polling_timeout

    def consume(self):
        while True:
            message = self.consumer.poll(
                self.polling_timeout)

            if message is None:
                continue
            if message.error():
                print(
                    "Consumer error: {}"
                    .format(message.error()))
                continue

            break

        return json.loads(message.value().decode('utf-8'))


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
                print(
                    "Failed to deliver message:"
                    "%s: %s" % (str(msg), str(err)))

        self.producer.produce(
            self.output_topic,
            json.dumps(message),
            callback=callback)
        self.producer.poll(self.polling_timeout)

    def consume(self, number_of_messages):
        messages = []
        for i in range(number_of_messages):
            while True:
                message = self.consumer.poll(
                    self.polling_timeout)

                if message is None:
                    continue
                if message.error():
                    print(
                        "Consumer error: {}"
                        .format(message.error()))
                    continue

                messages.append(
                    json.loads(
                        message.value().decode('utf-8')))
                break
        return messages
