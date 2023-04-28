import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from PIL import Image
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, plot_results, read_env
from ultralytics import YOLO
import os
import numpy as np
import json
import requests

env_config = read_env('../ENV.txt')
SAVE_RESULTS = env_config['AI_MODELS_SAVE_RESULTS'] == 'True'
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'
SENDING_METHOD = env_config['SENDING_METHOD']

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'test_server_listener'

print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)

running = True

def getDownloadLink(file_id):
    return "https://drive.google.com/uc?export=download&id=" + file_id

try:
    consumer.subscribe(['rawImageByte'])
    print('WAITING FOR IMAGES...')
    while running:
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
            print(msg.value())
            msg = json.loads(msg.value())

            img_data = requests.get(getDownloadLink(msg['file_id'])).content
            with open('image_name.jpg', 'wb') as handler:
                handler.write(img_data)


            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
