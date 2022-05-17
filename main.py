import threading
import numpy as np
from nodes import Manager, Worker, Admin


if __name__ == '__main__':
    server = 'localhost:9092'
    manager_group_id = 'manager-consumers'
    worker_group_id = 'worker-consumers'
    worker_parameters_topic = 'worker-parameters-topic'
    manager_parameters_topic = 'manager-parameters-topic'
    number_of_workers = 100
    number_of_partitions = 10
    replication_factor = 3

    admin = Admin(server=server)

    admin.create_topics(
        [worker_parameters_topic, manager_parameters_topic],
        number_of_partitions,
        replication_factor)

    def manager_loop():
        manager.consume(number_of_workers)

    def worker_loop():
        worker.produce(np.ones(2))

    manager = Manager(
        server=server,
        group_id=manager_group_id,
        input_topic=worker_parameters_topic,
        output_topic=manager_parameters_topic,
        run_function=manager_loop)

    workers = []
    for _ in range(number_of_workers):
        workers.append(Worker(
            server=server,
            group_id=worker_group_id,
            input_topic=manager_parameters_topic,
            output_topic=worker_parameters_topic,
            run_function=worker_loop))

    threads = [threading.Thread(target=manager.run, daemon=False)]

    for worker in workers:
        threads.append(threading.Thread(target=worker.run, daemon=False))

    for th in threads:
        th.start()

    for th in threads:
        th.join()

    admin.delete_topics([worker_parameters_topic, manager_parameters_topic])
