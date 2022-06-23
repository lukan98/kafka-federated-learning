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
            number_of_workers,
            polling_timeout,
            model,
            X,
            y
    ):
        self.communicator = Communicator(server, group_id, input_topic, output_topic, polling_timeout)
        self.number_of_workers = number_of_workers
        self.model = model
        self.X = X
        self.y = y

    def get_classification_report(self):
        return self.model.get_classification_report(self.X, self.y)

    def run(self):
        self.model.fit(self.X, self.y)

        iteration_counter = 0
        best_score = 0

        while True:
            messages = self.communicator.consume(self.number_of_workers)

            if all(map(lambda msg: msg == STOP_SIGNAL, messages)):
                break

            coefficients = list(
                map(lambda dict: deserialize_parameters(dict)[0],
                    messages))
            intercepts = list(
                map(lambda dict: deserialize_parameters(dict)[1],
                    messages))

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

            self.communicator.produce(
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
            polling_timeout,
            id,
            model,
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
        self.id = id
        self.model = model
        self.X_test = X_test
        self.y_test = y_test

    def run(self):
        self.model.fit(X=self.X_test, y=self.y_test)
        while True:
            data = self.data_consumer.consume()

            if data == STOP_SIGNAL:
                self.communicator.produce(STOP_SIGNAL)
                break

            X = np.array(data['X']).reshape(1, -1)
            y = np.ravel(np.array(data['y']), order='c')
            self.model.partial_fit(X=X, y=y)

            coefficients = self.model.get_coefficients()
            intercepts = self.model.get_intercepts()

            parameters = serialize_parameters(
                coefficients,
                intercepts)
            self.communicator.produce(parameters)

            aggregated_parameters = self.communicator.consume(1)[0]
            aggregated_coefficients, aggregated_intercepts = \
                deserialize_parameters(
                    aggregated_parameters)
            self.model.set_coefficients(
                aggregated_coefficients)
            self.model.set_intercepts(
                aggregated_intercepts)


class Admin:

    def __init__(self, server, verbose=False):
        self.admin_client = AdminClient({BOOTSTRAP_SERVER_KEY: server})
        self.verbose = verbose

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
                if self.verbose:
                    print(f'Topic {topic_name} successfully created!')
            except KafkaException:
                print(f'Failed to create topic {topic_name}')

    def delete_topics(self, topic_names):
        futures = self.admin_client.delete_topics(topic_names)

        for topic_name, future in futures.items():
            try:
                future.result()
                if self.verbose:
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
            self.producer.produce(
                data_dict,
                self.baseline_topic_name + '_' + worker_index)

        for worker_index in range(self.number_of_workers):
            self.producer.produce(
                STOP_SIGNAL,
                self.baseline_topic_name + '_' + str(worker_index))
