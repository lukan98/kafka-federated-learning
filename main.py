import threading
import time

import numpy as np
from nodes import Manager, Worker, Admin


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
    number_of_workers = 10
    number_of_partitions = 1
    replication_factor = 1

    setup_server(server)

    manager = Manager(
        server=server,
        group_id=manager_group_id,
        input_topic=worker_parameters_topic,
        output_topic=manager_parameters_topic,
        number_of_iterations=number_of_iterations,
        number_of_workers=number_of_workers)

    workers = []
    for worker_index in range(number_of_workers):
        workers.append(Worker(
            server=server,
            group_id=worker_group_id + str(worker_index),
            input_topic=manager_parameters_topic,
            output_topic=worker_parameters_topic,
            number_of_iterations=number_of_iterations,
            data=np.ones(1)))

    threads = [threading.Thread(target=manager.run, daemon=False)]

    for worker in workers:
        threads.append(threading.Thread(target=worker.run, daemon=False))

    for th in threads:
        th.start()

    for th in threads:
        th.join()
