# connect local flask server and get response
import requests
import sys
sys.path.append('../')
from utils import read_env, read_ccloud_config, get_image_data_from_bytes, getDriveDownloadLink, DriveAPI
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import os
from merger_thread import merger_thread_type as thread_type
import json
import threading
import shutil

env_config = read_env('../ENV.txt')

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
print('CLIENT CONFIG',client_config)
producer = Producer(client_config)

client_config['group.id'] = 'hardhatResults_4'
consumer_hardhat = Consumer(client_config)
consumer_hardhat.subscribe(['hardhatResults'])

client_config['group.id'] = 'smokerResults_4'
consumer_smoker = Consumer(client_config)
consumer_smoker.subscribe(['smokerResults'])

# stash that stores hardhat and age results temporarily
stash = {}

threading.Thread(target=thread_type, args=(consumer_smoker, producer, 'smoker_results', 'hardhatSmokerMerger', stash)).start()
threading.Thread(target=thread_type, args=(consumer_hardhat, producer, 'hardhat_results', 'hardhatSmokerMerger', stash)).start()
