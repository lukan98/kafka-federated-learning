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
            X_test,
            X_initial,
            y_test,
            y_initial,
            verbose=False
    ):
        self.communicator = Communicator(server, group_id, input_topic, output_topic, polling_timeout)
        self.number_of_workers = number_of_workers
        self.model = model
        self.X_test = X_test
        self.X_initial = X_initial
        self.y_test = y_test
        self.y_initial = y_initial
        self.verbose = verbose

    def get_classification_report(self):
        return self.model.get_classification_report(self.X_test, self.y_test)

    def run(self):
        # create the initial model
        self.model.fit(self.X_initial, self.y_initial)
        # send the initial model's parameters
        # to the workers
        self.communicator.produce(
            serialize_parameters(
                self.model.get_coefficients(),
                self.model.get_intercepts()))

        iteration_counter = 0
        best_score = self.model.score(
            self.X_test,
            self.y_test)
        # best coefficients and intercepts
        # are the parameters of the
        # global model at any given moment
        best_coefficients = self.model.get_coefficients()
        best_intercepts = self.model.get_intercepts()
        # FedAvg loop
        while True:
            # wait for all the workers
            # to send their local model's parameters
            messages = self.communicator.consume(
                self.number_of_workers)
            # if the workers have finished training
            # the models then stop the loop
            if all(map(
                    lambda msg: msg == STOP_SIGNAL,
                    messages)):
                break

            coefficients = list(
                map(lambda dict: deserialize_parameters(dict)[0],
                    messages))
            intercepts = list(
                map(lambda dict: deserialize_parameters(dict)[1],
                    messages))

            agg_coefficients = aggregate_parameters(
                coefficients)
            agg_intercepts = aggregate_parameters(
                intercepts)

            # update the model with the new parameters
            self.model.set_coefficients(agg_coefficients)
            self.model.set_intercepts(agg_intercepts)
            # test the newly updated global model
            score = self.model.score(
                self.X_test,
                self.y_test)

            if score > best_score:
                # if the new model is the best one yet
                # keep the new parameters and update
                # the best score and best parameters
                best_score = score
                best_coefficients = agg_coefficients
                best_intercepts = agg_intercepts
            else:
                # if the new model isn't the best one yet
                # restore the previous parameters
                self.model.set_coefficients(best_coefficients)
                self.model.set_intercepts(best_intercepts)

            if self.verbose:
                print(
                    f'Iteration {iteration_counter} score: {score}')
                print(
                    f'Best score: {best_score}')
            # send the workers the global model's parameters
            self.communicator.produce(
                serialize_parameters(
                    best_coefficients,
                    best_intercepts))

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
            model
    ):
        self.communicator = Communicator(
            server,
            group_id + str(id),
            input_topic,
            output_topic,
            polling_timeout)
        self.data_consumer = SampleConsumer(
            server,
            group_id + 'data' + str(id),
            data_topic + '_' + str(id),
            polling_timeout)
        self.id = id
        self.model = model

    def run(self):
        # wait for the global model's initial parameters
        initial_parameters = self.communicator.consume(1)[0]
        initial_coefficients, initial_intercepts = \
            deserialize_parameters(
                initial_parameters)
        # set the update the local model with the
        # initial global parameters
        self.model.set_coefficients(initial_coefficients)
        self.model.set_intercepts(initial_intercepts)

        while True:
            # receive training data
            data = self.data_consumer.consume()
            # the stop signal is received
            # when there's no more data
            if data == STOP_SIGNAL:
                # tell the manager that you're done
                self.communicator.produce(STOP_SIGNAL)
                break
            # parse training data into numpy array
            # reshaping and raveling to fix the array shape
            X = np.array(data['X']).reshape(1, -1)
            y = np.ravel(np.array(data['y']))
            # train the model with the single sample
            self.model.partial_fit(X=X, y=y)

            coefficients = self.model.get_coefficients()
            intercepts = self.model.get_intercepts()
            # serialize the model's parameters
            parameters = serialize_parameters(
                coefficients,
                intercepts)
            # send the parameters to the manager
            self.communicator.produce(parameters)
            # wait to receive the new global parameters
            global_parameters = self.communicator.consume(1)[0]
            global_coefficients, global_intercepts = \
                deserialize_parameters(
                    global_parameters)
            # update the local model
            self.model.set_coefficients(
                global_coefficients)
            self.model.set_intercepts(
                global_intercepts)


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


class DataStream:

    def __init__(self, server, baseline_topic_name, number_of_workers, polling_timeout, X, y):
        # create the producer
        self.producer = SampleProducer(server, polling_timeout)
        # indicates the prefix of every worker input topic
        # e.g. worker-input
        # the worker index is then appended to the baseline
        self.baseline_topic_name = baseline_topic_name
        self.number_of_workers = number_of_workers
        self.X = X
        self.y = y

    def run(self):
        for i in range(len(self.X)):
            # modulo operator for circular
            # distribution of data to every worker
            worker_index = str(i % self.number_of_workers)
            # create a dictionary out of a single sample
            data_dict = {
                'X': self.X[i].tolist(),
                'y': self.y[i].tolist()}
            self.producer.produce(
                data_dict,
                # e.g. worker-input_2
                self.baseline_topic_name +
                '_' +
                worker_index)

        # when there's no more data send all the workers
        # a message that they can stop working
        for worker_index in range(self.number_of_workers):
            self.producer.produce(
                STOP_SIGNAL,
                self.baseline_topic_name +
                '_' +
                str(worker_index))
