import threading

from communication import Communicator


class Manager:

    def __init__(self, server, group_id, input_topic, output_topic):
        self.communicator = Communicator(server, group_id, input_topic, output_topic, 1.0)

    def produce(self, message):
        self.communicator.produce(message)

    def consume(self, number_of_messages):
        messages = self.communicator.consume(number_of_messages)
        print(messages)


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

