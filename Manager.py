import threading

from communication import Communicator


class Manager:

    def __init__(self, server, input_topic, output_topic):
        self.communicator = Communicator(server, "manager_consumers", input_topic, output_topic, 1.0)

    def produce(self, message):
        self.communicator.produce(message)

    def consume(self):
        message = self.communicator.consume()
        print(message)


if __name__ == '__main__':
    manager = Manager(server='localhost:9092', input_topic='output', output_topic='output')

    def send_message():
        manager.produce('producer')

    def receive_message():
        manager.consume()

    threads = [threading.Thread(target=send_message, daemon=False), threading.Thread(target=receive_message, daemon=False)]

    for th in threads:
        th.start()

    for th in threads:
        th.join()

