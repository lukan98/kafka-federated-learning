import sys

from confluent_kafka import Producer


def delivery_callback(error, message):
    if error:
        sys.stderr.write('Message delivery failed!')
    if message:
        sys.stdout.write(
            f'Message delivery successful with: {message.topic()}, {message.partition()}, {message.offset()}')


if __name__ == '__main__':
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    topic = "test"

    producer.produce(topic, "Test value", callback=delivery_callback)

    producer.poll(0)
    producer.flush()
