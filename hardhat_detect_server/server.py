import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import json
import numpy as np
from PIL import Image
from io import BytesIO
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, plot_results, read_env
from ultralytics import YOLO
import os


env_config = read_env('../ENV.txt')
SAVE_RESULTS = False if env_config['AI_MODELS_SAVE_RESULTS'] == 'False' else False
ENABLE_UPSAMPLING = False if env_config['ENABLE_UPSAMPLING'] == 'False' else True
SENDING_METHOD = env_config['SENDING_METHOD']

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'hardhat_detect_server'

print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)

# check hardhat_detect folder exists in results folder
if not os.path.exists('../results/hardhat_detect'):
    os.makedirs('../results/hardhat_detect')

running = True
num = 0

# HARDHAT DETECT AI MODEL

model = YOLO("hardhat.pt")
model('test.png')
counter = 0


def predict_hardhat(image_path = None, image_data = None):
    # helmet : 0
    # vest : 1
    # head : 2
    # person : 3
    global counter
    print('PREDICTING HARDHAT...')
    results = None
    if image_path:
        results = model(image_path)
    if image_data:
        results = model(image_data)
    labels = {0: u'__background__', 1: u'helmet', 2: u'vest',3: u'head'}
    print('RES',results[0].boxes.boxes)
    result_image_data = None
    if image_path:
        result_image_data = plot_results(results, folder_path='../results/hardhat_detect/', image_path=image_path, labels=labels, result_name = 'hardhat_pred_' + str(counter) + '.jpg', save_image=True, return_image=True)
    else:
        result_image_data = plot_results(results, folder_path='../results/hardhat_detect/' ,image_data=image_data, labels=labels, result_name = 'hardhat_pred_' + str(counter) + '.jpg', save_image=True, return_image=True)
    if SAVE_RESULTS:
        result_image_data.save('../results/hardhat_detect/' + 'hardhat_pred_' + str(counter) + '.jpg')
    
    counter += 1
    print('PREDICTION', results[0].boxes.boxes)
    
    result_image_data = Image.fromarray(result_image_data)

    # SEND RESULTS TO KAFKA
    producer.produce('hardhatResults', key=str(counter), value=get_bytes_from_image_data(result_image_data))
    
    if SENDING_METHOD == 'flush':
        producer.flush()
    if SENDING_METHOD == 'poll':
        producer.poll(0)

    print('RESULT IMAGE SENT TO KAFKA')
        


try:
    if ENABLE_UPSAMPLING:
        consumer.subscribe(['upsampledPersonByte'])
    else:
        consumer.subscribe(['croppedPersonByte'])
    print('SUBSCRIBED TO TOPIC: croppedPersonByte')
    print('HARDHAT DETECT SERVER STARTED')
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
            img = get_image_data_from_bytes(msg.value())
            predict_hardhat(image_data=img)

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
