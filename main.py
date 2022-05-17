import threading
from components import Manager, Worker


if __name__ == '__main__':
    manager = Manager(
        server='localhost:9092',
        group_id='manager_consumers',
        input_topic='output',
        output_topic='output')

    def send_message():
        manager.produce('producer')

    def receive_message():
        manager.consume(5)

    threads = []
    for _ in range(100):
        threads.append(threading.Thread(target=send_message, daemon=False))

    for _ in range(20):
        threads.append(threading.Thread(target=receive_message, daemon=False))

    for th in threads:
        th.start()

    for th in threads:
        th.join()

