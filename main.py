import threading
import time
from sklearn.datasets import load_digits
from sklearn.model_selection import train_test_split
from nodes import Manager, Worker, Admin
from machine_learning import IrisClassifier, split_dataset, MNISTClassifier


def setup_server(server_name):
    admin = Admin(server=server_name)
    admin.delete_topics([worker_parameters_topic, manager_parameters_topic])
    time.sleep(1)
    admin.create_topics(
        [worker_parameters_topic, manager_parameters_topic],
        number_of_partitions,
        replication_factor)


if __name__ == '__main__':
    server = 'localhost:9092'
    manager_group_id = 'manager-consumers'
    worker_group_id = 'worker-consumers'
    worker_parameters_topic = 'worker-parameters-topic'
    manager_parameters_topic = 'manager-parameters-topic'
    number_of_iterations = 10
    number_of_workers = 2
    number_of_partitions = 1
    replication_factor = 1
    polling_timeout = 1

    X, y = load_digits(return_X_y=True)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    training_samples = split_dataset(X_train, y_train, number_of_workers, number_of_iterations)

    setup_server(server)

    manager = Manager(
        server=server,
        group_id=manager_group_id,
        input_topic=worker_parameters_topic,
        output_topic=manager_parameters_topic,
        number_of_iterations=number_of_iterations,
        number_of_workers=number_of_workers,
        polling_timeout=polling_timeout,
        model=MNISTClassifier(),
        X=X_test,
        y=y_test)

    workers = []
    for worker_index in range(number_of_workers):
        workers.append(Worker(
            server=server,
            group_id=worker_group_id,
            input_topic=manager_parameters_topic,
            output_topic=worker_parameters_topic,
            number_of_iterations=number_of_iterations,
            polling_timeout=polling_timeout,
            id=worker_index,
            model=MNISTClassifier(),
            training_data=training_samples[worker_index * number_of_iterations:(worker_index + 1) * number_of_iterations]))

    threads = [threading.Thread(target=manager.run, daemon=False)]

    for worker in workers:
        threads.append(threading.Thread(target=worker.run, daemon=False))

    for th in threads:
        th.start()

    for th in threads:
        th.join()
