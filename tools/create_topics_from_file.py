import sys
import os

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

"""
Create kafka topics from file contain topic names separated by new line.
Currently support default num_partitions=1, replication_factor=1.
Change kafka port by passing env variable KAFKA_PORT

Example:
KAFKA_PORT=49092 python3 create_topics_from_file.py testdata/topics
"""
KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
config = {
    'bootstrap_servers': f'localhost:{KAFKA_PORT}',
    'client_id': 'kafka-tool-01',
}


class Topic(NewTopic):
    def __init__(self, name: str, num_partitions: int = 1, replication_factor: int = 1):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.replica_assignments = {}
        self.topic_configs = {}


if len(sys.argv) < 2:
    raise Exception(f'Require one params: file contain topic names separated by new line')
file_name = sys.argv[1]


def create(topic_names: list):
    client = None
    try:
        client = KafkaAdminClient(**config)
        print(config)

        for name in topic_names:
            topic = Topic(name)
            try:
                client.create_topics(new_topics=[topic])
                print(f'Created topic: {name}')
            except TopicAlreadyExistsError as e1:
                print(f'Topic {name} exists => skip')
            except Exception as e:
                print(f'creating topic {name} failed: ' + e)
    except Exception as e:
        print(e)
    finally:
        if client:
            client.close()


def main():
    with open(file_name) as file:
        topics = [line.strip() for line in file.readlines()]
        create(topics)


if __name__ == '__main__':
    main()
