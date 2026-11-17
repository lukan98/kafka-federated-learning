# Federated Learning using Kafka and Python

A federated-learning simulation built on Apache Kafka. A central **manager** aggregates model parameters from many **workers** using Federated Averaging (FedAvg), while the training data is streamed to the workers over Kafka topics.

The project demonstrates how a machine-learning workflow can be distributed over a message broker without moving raw training data to the coordinator.

## How it works

- A `DataStream` producer sends individual training samples to per-worker topics.
- Each `Worker` trains a local scikit-learn MLP classifier on the samples it receives.
- After each sample, a worker sends its model parameters to the manager.
- The `Manager` averages the received parameters (FedAvg), evaluates the global model, keeps the best version, and sends it back to the workers.
- When the data stream ends, workers send a stop signal and the manager prints a classification report.

The dataset is scikit-learn's built-in handwritten-digits dataset. The default configuration uses 50 workers.

## Prerequisites

1. Python 3.11.9 (pinned in `.tool-versions`; the code requires 3.8 or newer)
2. Java 8 or newer (required by Kafka)
3. Apache Kafka with Zookeeper

Download Kafka, for example version 3.1.0, and extract it:

```bash
tar -xzf kafka_2.13-3.1.0.tgz
export KAFKA_DIR="$PWD/kafka_2.13-3.1.0"
```

## Python dependencies

- `confluent-kafka`
- `numpy`
- `pandas`
- `scikit-learn`
- `matplotlib` (only needed for `graphs.py`)

Install the pinned Python version and dependencies:

```bash
asdf install
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running

1. Start Zookeeper:

   ```bash
   "$KAFKA_DIR"/bin/zookeeper-server-start.sh "$KAFKA_DIR"/config/zookeeper.properties
   ```

2. In another terminal, start the Kafka broker:

   ```bash
   "$KAFKA_DIR"/bin/kafka-server-start.sh "$KAFKA_DIR"/config/server.properties
   ```

3. In a third terminal, start the simulation from this directory:

   ```bash
   python3 main.py
   ```

`main.py` deletes and recreates all required Kafka topics on startup. Training runs until the data stream is exhausted; the final classification report is printed to standard output.

## Project structure

```text
main.py              # experiment setup: topics, manager, workers, data stream
constants.py         # shared Kafka message keys and stop signal
communication.py     # producers, consumers and the Communicator abstraction
nodes.py             # Manager, Worker, Admin and DataStream implementations
machine_learning.py  # digit classifier, dataset preparation and FedAvg aggregation
graphs.py            # optional plots of accuracy and runtime versus worker count
```

## Notes

- The simulation uses `localhost:9092` as the Kafka bootstrap server.
- The number of workers is set in `main.py`; increasing it also creates more topics and threads.
- `graphs.py` contains precomputed plotting data from earlier experiments and is not part of the training loop.
