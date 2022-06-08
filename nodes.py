from machine_learning import *
from communication import *
from constants import *
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic


class Manager:

    def __init__(
            self,
            server,
            group_id,
            input_topic,
            output_topic,
            number_of_iterations,
            number_of_workers,
            polling_timeout,
            model,
            X,
            y
    ):
        self.communicator = Communicator(server, group_id, input_topic, output_topic, polling_timeout)
        self.number_of_iterations = number_of_iterations
        self.number_of_workers = number_of_workers
        self.model = model
        self.X = X
        self.y = y

    def produce(self, message):
        self.communicator.produce(message)

    def consume(self, number_of_messages):
        return self.communicator.consume(number_of_messages)

    def run(self):
        self.model.fit(self.X, self.y)
        for iteration in range(self.number_of_iterations):
            parameters = self.consume(self.number_of_workers)
            coefficients = list(map(lambda parameter_dict: deserialize_parameters(parameter_dict)[0], parameters))
            intercepts = list(map(lambda parameter_dict: deserialize_parameters(parameter_dict)[1], parameters))

            aggregated_coefficients = aggregate_parameters(coefficients)
            aggregated_intercepts = aggregate_parameters(intercepts)

            self.model.set_coefficients(aggregated_coefficients)
            self.model.set_intercepts(aggregated_intercepts)

            print(f'Iteration {iteration + 1}, score: {self.model.score(self.X, self.y)}')

            # print(f'Iteration {iteration}\n'
            #       f'Number of messages: {len(coefficients)}\n'
            #       f'Coefficients: {aggregated_coefficients}\n'
            #       f'Intercepts: {aggregated_intercepts}\n')

            self.produce(serialize_parameters(aggregated_coefficients, aggregated_intercepts))


class Worker:

    def __init__(
            self,
            server,
            group_id,
            input_topic,
            output_topic,
            number_of_iterations,
            polling_timeout,
            id,
            model,
            training_data
    ):
        self.communicator = Communicator(server, group_id + str(id), input_topic, output_topic, polling_timeout)
        self.number_of_iterations = number_of_iterations
        self.id = id
        self.model = model
        self.training_data = training_data

    def produce(self, message):
        self.communicator.produce(message)

    def consume(self, number_of_messages):
        return self.communicator.consume(number_of_messages)

    def run(self):
        for iteration in range(self.number_of_iterations):
            X, y = self.training_data[iteration]
            if iteration == 0:
                self.model.fit(X=X, y=y)
            else:
                self.model.partial_fit(X=X, y=y)
            coefficients = self.model.get_coefficients()
            intercepts = self.model.get_intercepts()

            # print(f'Worker parameters\n'
            #       f'Coefficients: {coefficients}\n'
            #       f'Intercepts: {intercepts}\n')
            parameters = serialize_parameters(coefficients, intercepts)
            self.produce(parameters)

            aggregated_parameters = self.consume(1)[0]
            aggregated_coefficients, aggregated_intercepts = deserialize_parameters(aggregated_parameters)
            self.model.set_coefficients(aggregated_coefficients)
            self.model.set_intercepts(aggregated_intercepts)


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
