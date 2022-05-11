from communication import Communicator


class Worker:

    def __init__(self, server, group_id, input_topic, output_topic):
        self.communicator = Communicator(server, group_id, input_topic, output_topic, 1.0)

    def produce(self, message):
        self.communicator.produce(message)

    def consume(self):
        message = self.communicator.consume()
        print(message)