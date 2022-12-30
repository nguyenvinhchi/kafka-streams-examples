import sys
import os

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

"""
Delete kafka topics from file contain topic names separated by new line.
Currently support default num_partitions=1, replication_factor=1.
Change kafka port by passing env variable KAFKA_PORT

Example:
KAFKA_PORT=49092 python3 delete_topics_from_file.py testdata/topics
"""

# Disable passing kafka port from env because we only want to delete topics in local env which port should always be 9092
# KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
KAFKA_PORT = '9092'
config = {
    'bootstrap_servers': f'localhost:{KAFKA_PORT}',
    'client_id': 'kafka-tool-01',
}


if len(sys.argv) < 2:
    raise Exception(f'Require one params: file contain topic names separated by new line')
file_name = sys.argv[1]


def delete(topic_names: list):
    try:
        client = KafkaAdminClient(**config)
        # print(config)

        for name in topic_names:
            try:
                client.delete_topics(topics=[name])
                print(f'Deleted topic: {name}')
            except UnknownTopicOrPartitionError as e1:
                print(f'Topic {name} not exist => skip')
            except Exception as e:
                print(f'creating topic {name} failed: ' + e)
    except Exception as e:
        print(e)
    finally:
        client.close()


def main():
    with open(file_name) as file:
        topics = [line.strip() for line in file.readlines()]
        delete(topics)


if __name__ == '__main__':
    main()
