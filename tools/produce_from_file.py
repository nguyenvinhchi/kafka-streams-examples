import datetime
import sys
from time import time
from kafka import KafkaProducer
import json
import re
import os
import unittest


"""
Produce messages from a text file separated by new lines,
file name will be the topic name to produce msg

Requires: pip install kafka-python

usage example:
python3 produce_msg_from_file.py /home/chinv/chinguyen/unified/go/src/github.com/Unified/kafka-streams-enrichment/dev-chinv/snapchat/data/events-static/4_events.snapchat-segment-v1

HOW TO VERIFY:

kafka-consume test --property print.key=true
"""

KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')


def initialize():
    print(f'USE KAFKA PORT: {KAFKA_PORT}')
    if len(sys.argv) < 2:
        raise Exception('Reuquires file path containing messages separated by new line')
    file_path = sys.argv[1]
    file_name = file_path.split("/")[-1]
    m = re.search("((\d+)_)?(.*)(_keyed)?", file_name)
    topic_name: str = m.group(3)
    using_key = False
    if topic_name.endswith('_keyed'):
        using_key = True
        topic_name = topic_name[0: len(topic_name) - len('_keyed')]
    print(f'File contain messages: {file_path}')
    print(f'topic name: {topic_name}')
    print(f'Producing messages with keyed? {using_key}')
    return file_path, topic_name, using_key


def read_file(file_path) -> list:
    messages = list()
    with open(file_path) as file:
        while True:
            line = file.readline()
            if len(line) == 0:
                break
            messages.append(line.strip())
    print(f'Num of msg: {len(messages)}')
    return messages


def get_msg(msg, using_key):
    if using_key:
        m2 = re.search("(.+?)\\s*:\\s*(\{.+\})$", msg)
        if not m2:
            raise Exception("Invalid keyed:value message, correct one should be like this------ key1:{'k1':'v1', 'k2':'v2'}")

        k, v = m2.group(1), m2.group(2)
        return k, v
    else:
        return None, msg


def send():
    file_path, topic_name, using_key = initialize()
    producer = KafkaProducer(bootstrap_servers=[f'127.0.0.1:{KAFKA_PORT}'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for idx, msg in enumerate(read_file(file_path)):
        k, v = get_msg(msg, using_key)
        try:
            message = json.loads(v)
        except:
            message = v

        if not k:
            k = str(datetime.datetime.now().timestamp())
        producer.send(topic_name, key=str(k).encode('utf-8'), value=message)
        if idx == 10:
            producer.flush()
    producer.flush()


def assertEqual(expected, actual):
    if expected == actual:
        print("Passed")
    else:
        raise Exception(f"actual = `{actual}`, IS NOT expected as `{expected}`")


def _test_get_msg_using_key():
    msg = '''campaign_qa_ondemand_123: { "job": { "accountID": "87a19740-5e5f-46ae-99b5-a6896209ffb0", "accountName": "Unified adAccount", "accountTimezone": "America/Los_Angeles", "archived": false, "brandCompanyID": "b990667d-05d4-42f1-a513-db8f9dc18e8c", "brandCompanyName": "Programmatic Client Demo", "brandID": "ce6e9e84-5249-4509-a992-1900f4966aff", "brandName": "Programmatic Client Demo Brand", "campaigns": [ { "campaignID": "14eca417-ae5a-44b2-9ec6-97fc37fb221b" } ], "companyID": "fea1aeee-d0d2-4992-aaa6-01fc3db4d463", "companyName": "Unified (Managed Service)", "completed": false, "creatorID": "dheide@unified.com", "currency": "USD", "dateCreated": "2021-08-04T16:47:04Z", "dateModified": "2021-08-04T16:47:04Z", "deliveryStatus": "Current", "deliveryTarget": 75002, "disabled": false, "endDate": "2021-08-31T23:59:59-07:00", "fee": 75.03, "feePercentage": 15, "feeType": "Percent of Spend", "foreignID": "a0H5Y00001KamLWUAZ", "foreignStatus": "Deployed", "id": "8950278f-3edb-4528-ab17-08a23ed2722e", "initiativeCode": "a0E1O00000aL3vWUAS", "initiativeID": "a034c54a-286d-4aba-9ceb-cb2334d2e665", "initiativeName": "IHM-Demo IHM Market-Programmatic Client Demo-IO - Programmatic Client Demo_Unified Cares --000000", "isContinuous": false, "jobInstanceId": "campaign_qa_ondemand_123", "localEndDate": "2021-08-31", "localStartDate": "2020-05-03", "mediaSpend": 425.17, "mediaType": "Paid", "name": "IHMket_IO - Programmatic Client Demo_Unified Cares -_Facebook QA Markets test155_Facebook_5.3.2020_8.31.2021", "pacingType": "Impression", "programaticMediaType": "SnapChat", "programaticPlatform": "Social", "publisher": "snapchat", "startDate": "2020-05-03T00:00:00-07:00", "totalBudget": 500.2 } }'''
    k, v = get_msg(msg, using_key=True)
    print(k)
    print(v)
    assertEqual('campaign_qa_ondemand_123', k)


if __name__ == '__main__':
    # print(str(datetime.datetime.now().timestamp()*1000).split('.')[0])
    send()
    # _test_get_msg_using_key()
