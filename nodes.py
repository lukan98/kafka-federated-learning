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

    def get_classification_report(self):
        return self.model.get_classification_report(self.X, self.y)

    def run(self):
        self.model.fit(self.X, self.y)

        iteration_counter = 0
        best_score = 0

        while True:
            parameters = self.consume(self.number_of_workers)

            coefficients = list(
                map(lambda parameter_dict: deserialize_parameters(parameter_dict)[0],
                    parameters))
            intercepts = list(
                map(lambda parameter_dict: deserialize_parameters(parameter_dict)[1],
                    parameters))

            aggregated_coefficients = aggregate_parameters(coefficients)
            aggregated_intercepts = aggregate_parameters(intercepts)

            old_coefficients = self.model.get_coefficients()
            old_intercepts = self.model.get_intercepts()

            self.model.set_coefficients(aggregated_coefficients)
            self.model.set_intercepts(aggregated_intercepts)
            score = self.model.score(self.X, self.y)

            if score > best_score:
                best_score = score
            else:
                self.model.set_coefficients(old_coefficients)
                self.model.set_intercepts(old_intercepts)

            print(f'Iteration {iteration_counter} score: {score}')
            print(f'Best score: {best_score}')

            self.produce(
                serialize_parameters(
                    aggregated_coefficients,
                    aggregated_intercepts))

            iteration_counter += 1


class Worker:

    def __init__(
            self,
            server,
            group_id,
            data_topic,
            input_topic,
            output_topic,
            number_of_iterations,
            polling_timeout,
            id,
            model,
            training_data,
            X_test,
            y_test
    ):
        self.communicator = Communicator(
            server,
            group_id + str(id),
            input_topic,
            output_topic,
            polling_timeout)
        self.data_consumer = FLConsumer(
            server,
            group_id + 'data' + str(id),
            data_topic + '_' + str(id),
            polling_timeout)
        self.number_of_iterations = number_of_iterations
        self.id = id
        self.model = model
        self.training_data = training_data
        self.X_test = X_test
        self.y_test = y_test

    def produce(self, message):
        self.communicator.produce(message)

    def consume(self, number_of_messages):
        return self.communicator.consume(number_of_messages)

    def consume_data(self):
        return self.data_consumer.consume()

    def run(self):
        self.model.fit(X=self.X_test, y=self.y_test)
        while True:
            data_dict = self.consume_data()

            X = np.array(data_dict['X']).reshape(1, -1)
            y = np.ravel(np.array(data_dict['y']), order='c')
            self.model.partial_fit(X=X, y=y)

            coefficients = self.model.get_coefficients()
            intercepts = self.model.get_intercepts()

            parameters = serialize_parameters(
                coefficients,
                intercepts)
            self.produce(parameters)

            aggregated_parameters = self.consume(1)[0]
            aggregated_coefficients, aggregated_intercepts = \
                deserialize_parameters(
                    aggregated_parameters)
            self.model.set_coefficients(
                aggregated_coefficients)
            self.model.set_intercepts(
                aggregated_intercepts)


class Admin:

    def __init__(self, server):
        self.admin_client = AdminClient({BOOTSTRAP_SERVER_KEY: server})

    def create_topics(self, topic_names, number_of_partitions, replication_factor):
        futures = self.admin_client.create_topics(
            [NewTopic(
                topic_name,
                number_of_partitions,
                replication_factor)
                for topic_name in topic_names])

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


class DataProducer:

    def __init__(self, server, baseline_topic_name, number_of_workers, polling_timeout, X, y):
        self.producer = FLProducer(server, polling_timeout)
        self.baseline_topic_name = baseline_topic_name
        self.number_of_workers = number_of_workers
        self.X = X
        self.y = y

    def run(self):
        for i in range(len(self.X)):
            worker_index = str(i % self.number_of_workers)
            data_dict = {'X': self.X[i].tolist(), 'y': self.y[i].tolist()}
            self.producer.produce(data_dict, self.baseline_topic_name + '_' + worker_index)
