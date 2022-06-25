import threading
import time
import warnings
from sklearn.exceptions import DataConversionWarning
from nodes import Manager, Worker, Admin, DataStream
from machine_learning import DigitClassifier, make_digit_datasets

warnings.filterwarnings(action='ignore', category=DataConversionWarning)


def setup_server(server_name, topic_names):
    admin = Admin(server=server_name)
    admin.delete_topics(topic_names)
    time.sleep(1)
    admin.create_topics(
        topic_names,
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

    number_of_workers = 50

    number_of_partitions = 1
    replication_factor = 1
    polling_timeout = 1

    for i in range(number_of_workers):
        topics.append(worker_input_topic + '_' + str(i))

    setup_server(server, topics)

    X_train, X_test, X_initial, y_train, y_test, y_initial = make_digit_datasets(
        number_of_workers,
        initial_samples_per_class=5)

    data_producer = DataStream(
        server=server,
        baseline_topic_name=worker_input_topic,
        number_of_workers=number_of_workers,
        polling_timeout=polling_timeout,
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
