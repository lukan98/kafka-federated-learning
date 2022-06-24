import threading
import time
from sklearn.datasets import load_digits
from sklearn.model_selection import train_test_split
from nodes import Manager, Worker, Admin, DataStream
from machine_learning import cutoff_dataset, DigitClassifier


def setup_server(server_name, topics):
    admin = Admin(server=server_name)
    admin.delete_topics(topics)
    time.sleep(1)
    admin.create_topics(
        topics,
        number_of_partitions,
        replication_factor)


def make_datasets(X, y, number_of_workers, test_size=0.2, initial_size=0.05):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    X_train, X_initial, y_train, y_initial = train_test_split(X_train, y_train, test_size=0.05)
    X_train, y_train = cutoff_dataset(X_train, y_train, number_of_workers)

    return X_train, X_test, X_initial, y_train, y_test, y_initial


if __name__ == '__main__':
    server = 'localhost:9092'

    manager_group_id = 'manager-consumers'
    worker_group_id = 'worker-consumers'

    worker_parameters_topic = 'worker-parameters-topic'
    manager_parameters_topic = 'manager-parameters-topic'
    worker_input_topic = 'worker_input'

    topics = [worker_parameters_topic, manager_parameters_topic]

    number_of_workers = 100

    number_of_partitions = 1
    replication_factor = 1
    polling_timeout = 1

    for i in range(number_of_workers):
        topics.append(worker_input_topic + '_' + str(i))

    setup_server(server, topics)

    X, y = load_digits(return_X_y=True)
    X_train, X_test, X_initial, y_train, y_test, y_initial = make_datasets(X, y, number_of_workers)

    data_producer = DataStream(
        server=server,
        baseline_topic_name=worker_input_topic,
        number_of_workers=number_of_workers,
        polling_timeout=1.0,
        X=X_train,
        y=y_train)

    manager = Manager(
        server=server,
        group_id=manager_group_id,
        input_topic=worker_parameters_topic,
        output_topic=manager_parameters_topic,
        number_of_workers=number_of_workers,
        polling_timeout=polling_timeout,
        model=DigitClassifier(),
        X_test=X_test,
        X_initial=X_initial,
        y_test=y_test,
        y_initial=y_initial,
        verbose=True)

    workers = []
    for worker_index in range(number_of_workers):
        model = DigitClassifier()
        model.fit(X=X_initial, y=y_initial)

        workers.append(Worker(
            server=server,
            group_id=worker_group_id,
            data_topic=worker_input_topic,
            input_topic=manager_parameters_topic,
            output_topic=worker_parameters_topic,
            polling_timeout=polling_timeout,
            id=worker_index,
            model=model))

    threads = [
        threading.Thread(target=data_producer.run, daemon=False),
        threading.Thread(target=manager.run, daemon=False)
    ]

    for worker in workers:
        threads.append(threading.Thread(target=worker.run, daemon=False))

    start_time = time.time()

    for th in threads:
        th.start()

    for th in threads:
        th.join()

    end_time = time.time()

    print("--- %s seconds ---" % (time.time() - start_time))

    print(manager.get_classification_report())
