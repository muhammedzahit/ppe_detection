import sys
sys.path.append('../')
from confluent_kafka import Consumer
import json
import numpy as np
from PIL import Image
from io import BytesIO
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, plot_results, read_env
from ultralytics import YOLO


env_config = read_env('../ENV.txt')
SAVE_RESULTS = True
ENABLE_UPSAMPLING = False if env_config['ENABLE_UPSAMPLING'] == 'False' else True

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'hardhat_detect_server'

print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)

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
    if SAVE_RESULTS:
        if image_path:
            plot_results(results, folder_path='../results/hardhat_detect/', image_path=image_path, labels=labels, result_name = 'hardhat_pred_' + str(counter) + '.jpg')
        else:
            plot_results(results, folder_path='../results/hardhat_detect/' ,image_data=image_data, labels=labels, result_name = 'hardhat_pred_' + str(counter) + '.jpg')
    counter += 1
    print('PREDICTION', results[0].boxes.boxes)



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
