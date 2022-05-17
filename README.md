# Federated Learning using Kafka and Python

## Prerequisites
1. Python 3
2. Apache Kafka\
Simply [download](https://www.apache.org/dyn/closer.cgi?path=/kafka/3.1.0/kafka_2.13-3.1.0.tgz) and untar the file.
~~~
$ tar -xzf kafka_2.13-3.1.0.tgz
~~~

## Python package dependencies
1. `confluent-kafka`
2. `numpy`
3. `scikit-learn`
4. `matplotlib`

## Running instructions

1. Run Zookeeper
~~~
$ "$KAFKA_DIR"/bin/zookeeper-server-start.sh config/zookeeper.properties
~~~
2. Run Kafka server
~~~
$ "$KAFKA_DIR"/bin/kafka-server-start.sh config/server.properties
~~~
