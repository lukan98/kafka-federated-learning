from communication import *
from constants import *
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic
import numpy as np


class Manager:

    def __init__(
            self,
            server,
            group_id,
            input_topic,
            output_topic,
            number_of_iterations,
            number_of_workers,
            polling_timeout
    ):
        self.communicator = Communicator(server, group_id, input_topic, output_topic, polling_timeout)
        self.number_of_iterations = number_of_iterations
        self.number_of_workers = number_of_workers

    def produce(self, message):
        self.communicator.produce(message)

    def consume(self, number_of_messages):
        return self.communicator.consume(number_of_messages)

    def run(self):
        for iteration in range(self.number_of_iterations):
            messages = self.consume(self.number_of_workers)
            aggregation = np.sum(messages)
            print(f'Iteration {iteration} result: {aggregation}')
            self.produce(aggregation)


class Worker:

    def __init__(
            self,
            server,
            group_id,
            input_topic,
            output_topic,
            number_of_iterations,
            data,
            polling_timeout
    ):
        self.communicator = Communicator(server, group_id, input_topic, output_topic, polling_timeout)
        self.number_of_iterations = number_of_iterations
        self.data = data

    def produce(self, message):
        self.communicator.produce(message)

    def consume(self, number_of_messages):
        return self.communicator.consume(number_of_messages)

    def run(self):
        for iteration in range(self.number_of_iterations):
            self.produce(self.data)
            message = self.consume(1)
            self.data = message


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
