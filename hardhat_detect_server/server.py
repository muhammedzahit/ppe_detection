import json
import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from PIL import Image
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, plot_results, read_env, \
DriveAPI, getDriveDownloadLink, downloadImageFromURL, getImageDataFromDriveFileId
from ultralytics import YOLO
import os
import numpy as np


env_config = read_env('../ENV.txt')
SAVE_RESULTS = env_config['AI_MODELS_SAVE_RESULTS'] == 'True'
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'
SENDING_METHOD = env_config['SENDING_METHOD']

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'hardhat_detect_server'
client_config['message.max.bytes'] = 32000000
client_config['fetch.message.max.bytes'] = 32000000


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

# Connect Google Drive API
driveAPI = DriveAPI('../credentials.json')


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
    result_image_data = None
    if image_path:
        result_image_data = plot_results(results, folder_path='../results/hardhat_detect/', image_path=image_path, labels=labels, result_name = 'hardhat_pred_' + str(counter) + '.jpg', save_image=True, return_image=True)
    else:
        result_image_data = plot_results(results, folder_path='../results/hardhat_detect/' ,image_data=image_data, labels=labels, result_name = 'hardhat_pred_' + str(counter) + '.jpg', save_image=True, return_image=True)
    
    # save ndarray image data to jpg file
    im = Image.fromarray(result_image_data)
    im.save('temp.jpg')

    if SAVE_RESULTS:
        if not os.path.exists('../results/hardhat_detect'):
            os.makedirs('../results/hardhat_detect')

        im.save('../results/hardhat_detect/' + 'hardhat_pred_' + str(counter) + '.jpg')

    counter += 1

    # SEND RESULTS TO GOOGLE DRIVE
    file_id = driveAPI.FileUpload('temp.jpg', 'hardhat_pred_' + str(counter) + '.jpg', folder_id='1Q4sb2KoVRk2jbi-WEHElK47g_09ARdGR')

    # SEND RESULTS TO KAFKA
    value_ = {'file_id' : file_id, 'key' : 'hardhat_pred_' + str(counter) + '.jpg'}
    producer.produce('hardhatResults', key=str(counter), value=json.dumps(value_))

    if SENDING_METHOD == 'flush':
        producer.flush()
    if SENDING_METHOD == 'poll':
        producer.poll(0)

    print('RESULT IMAGE SENT TO KAFKA')    


try:
    if ENABLE_UPSAMPLING:
        consumer.subscribe(['upsampledPersonByte'])
        print('SUBSCRIBED TO TOPIC: upsampledPersonByte')
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
            msg_json = json.loads(msg.value().decode('utf-8'))
            print('MESSAGE RECEIVED IN HARDHAT DETECT SERVER: ', msg_json)

            predict_hardhat(image_data=getImageDataFromDriveFileId(driveAPI,msg_json['file_id']))

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
