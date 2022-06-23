import threading
import time
from sklearn.datasets import load_digits, load_iris
from sklearn.model_selection import train_test_split
from nodes import Manager, Worker, Admin, DataProducer
from machine_learning import split_dataset, DigitClassifier


def setup_server(server_name, topics):
    admin = Admin(server=server_name)
    admin.delete_topics(topics)
    time.sleep(1)
    admin.create_topics(
        topics,
        number_of_partitions,
        replication_factor)


if __name__ == '__main__':
    server = 'localhost:9092'

    manager_group_id = 'manager-consumers'
    worker_group_id = 'worker-consumers'

    worker_parameters_topic = 'worker-parameters-topic'
    manager_parameters_topic = 'manager-parameters-topic'
    worker_input_topic = 'worker_input'

    topics = [worker_parameters_topic, manager_parameters_topic]

    number_of_iterations = 2
    number_of_workers = 10

    number_of_partitions = 1
    replication_factor = 1
    polling_timeout = 1

    for i in range(number_of_workers):
        topics.append(worker_input_topic + '_' + str(i))

    setup_server(server, topics)

    X, y = load_iris(return_X_y=True)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1)
    training_samples = split_dataset(X_train, y_train, number_of_workers, number_of_iterations)

    data_producer = DataProducer(
        server=server,
        baseline_topic_name=worker_input_topic,
        number_of_workers=number_of_workers,
        polling_timeout=1.0,
        X=X,
        y=y)

    workers = []
    for worker_index in range(number_of_workers):
        workers.append(Worker(
            server=server,
            group_id=worker_group_id,
            data_topic=worker_input_topic,
            input_topic=manager_parameters_topic,
            output_topic=worker_parameters_topic,
            number_of_iterations=number_of_iterations,
            polling_timeout=polling_timeout,
            id=worker_index,
            model=DigitClassifier(),
            training_data=training_samples[
                          worker_index * number_of_iterations:(worker_index + 1) * number_of_iterations],
            X_test=X_test,
            y_test=y_test
        ))

    threads = [threading.Thread(target=data_producer.run, daemon=False)]

    for worker in workers:
        threads.append(threading.Thread(target=worker.run, daemon=False))

    start_time = time.time()

    for th in threads:
        th.start()

    for th in threads:
        th.join()
