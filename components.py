from communication import Communicator
from constants import *
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic


class Manager:

    def __init__(self, server, group_id, input_topic, output_topic, run_function):
        self.communicator = Communicator(server, group_id, input_topic, output_topic, 1.0)
        self.run = run_function

    def produce(self, message):
        self.communicator.produce(message)

    def consume(self, number_of_messages):
        messages = self.communicator.consume(number_of_messages)
        print(messages)


class Worker:

    def __init__(self, server, group_id, input_topic, output_topic, run_function):
        self.communicator = Communicator(server, group_id, input_topic, output_topic, 1.0)
        self.run = run_function

    def produce(self, message):
        self.communicator.produce(message)

    def consume(self, number_of_messages):
        messages = self.communicator.consume(number_of_messages)
        print(messages)


class Admin:

    def __init__(self, server):
        self.admin_client = AdminClient({BOOTSTRAP_SERVER_KEY: server})

    def create_topics(self, topic_names, number_of_partitions, replication_factor):
        futures = self.admin_client.create_topics(
            [NewTopic(topic_name, number_of_partitions, replication_factor) for topic_name in topic_names])

        for topic_name, future in futures.items():
            try:
                future.result()
                print(f'Topic {topic_name} successfully created!')
            except KafkaException:
                print(f'Failed to create topic {topic_name}')

    def delete_topics(self, topic_names):
        futures = self.admin_client.delete_topics(topic_names)

        for topic_name, future in futures.items():
            try:
                future.result()
                print(f'Topic {topic_name} successfully deleted!')
            except KafkaException:
                print(f'Failed to delete topic {topic_name}')