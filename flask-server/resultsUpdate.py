# connect local flask server and get response
import requests
import sys
sys.path.append('../')
from utils import read_env, read_ccloud_config, get_image_data_from_bytes, get_bytes_from_image_data
from confluent_kafka import Producer, Consumer
import os
import pickle
import urllib3

env_config = read_env('../ENV.txt')

# connect to local flask server
url = env_config['FLASK_SERVER_URL']

def updateResults2(results):
    # post request to FLASK_SERVER_URL/updateResults with byte image
    print(url + 'updateResults')
    r = requests.post(url + 'updateResults', json=results)
    print(r.json())

def updateResults(byte_img):
    # post request to FLASK_SERVER_URL/updateResults with byte image
    http = urllib3.PoolManager()
    r = http.request('POST', url + 'updateResults/hardhat_results', body=byte_img, headers={'Content-Type': 'application/octet-stream'})  
    

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
print('CLIENT CONFIG',client_config)
producer = Producer(client_config)

client_config['group.id'] = 'monitoringPage_rawImageByte'
consumer = Consumer(client_config)
consumer.subscribe(['rawImageByte'])

print('WAITING FOR UPLOADED IMAGE')
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None: 
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
            (msg.topic(), msg.partition(), msg.offset()))
        elif msg.error():
            raise KafkaException(msg.error())
    else:
        #msg = msg.value().decode('utf-8')
        print('IMAGE RECEIVED')
        updateResults(msg.value())
            