# connect local flask server and get response
import requests
import sys
sys.path.append('../')
from utils import read_env, read_ccloud_config, get_image_data_from_bytes, getDriveDownloadLink, DriveAPI
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import os
import json
import threading
import shutil

env_config = read_env('../ENV.txt')

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
print('CLIENT CONFIG',client_config)
producer = Producer(client_config)

client_config['group.id'] = 'ageSmokerMerger'
consumer_ageSmoker = Consumer(client_config)
consumer_ageSmoker.subscribe(['ageSmokerMerger'])

client_config['group.id'] = 'hardhatAgeMerger'
consumer_hardhatAge = Consumer(client_config)
consumer_hardhatAge.subscribe(['hardhatAgeMerger'])

client_config['group.id'] = 'hardhatSmokerMerger'
consumer_hardhatSmoker = Consumer(client_config)
consumer_hardhatSmoker.subscribe(['hardhatSmokerMerger'])

stash_temp = {}
stash = {}

def checkMessage(msg):
    if msg is None: 
        return False
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
            (msg.topic(), msg.partition(), msg.offset()))
        elif msg.error():
            raise KafkaException(msg.error())
    return True

def thread_type(consumer, consumer_type):
    print('THREAD STARTED', consumer_type)
    while True:
        msg = consumer.poll(timeout=1.0)
        if checkMessage(msg):
            #msg = msg.value().decode('utf-8')
            msg_json = json.loads(msg.value().decode('utf-8'))
            
            if msg_json['parent_image_id'] in stash:
                continue

            if msg_json['parent_image_id'] not in stash_temp:
                stash_temp[msg_json['parent_image_id']] = msg_json
            else:

                # merge the two dicts
                stash_temp[msg_json['parent_image_id']].update(msg_json)
                # store the merged dict in stash
                stash[msg_json['parent_image_id']] = stash_temp[msg_json['parent_image_id']]

                print('-'*50)
                print(stash[msg_json['parent_image_id']])
                print('-'*50)
                # SEND TO KAFKA
                producer.produce(topic='finalMerger', value=json.dumps(stash[msg_json['parent_image_id']]))


threading.Thread(target=thread_type, args=(consumer_ageSmoker, 'ageSmokerMerger')).start()
threading.Thread(target=thread_type, args=(consumer_hardhatAge, 'hardhatAgeMerger')).start()
threading.Thread(target=thread_type, args=(consumer_hardhatSmoker, 'hardhatSmokerMerger')).start()
