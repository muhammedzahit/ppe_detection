import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer
import json
import numpy as np
from PIL import Image
from io import BytesIO
from utils import read_ccloud_config, get_image_data_from_bytes, read_env, plot_results, get_bytes_from_image_data, \
DriveAPI, downloadImageFromURL, getDriveDownloadLink, getImageDataFromDriveFileId
import tensorflow as tf
from tensorflow.keras.models import load_model
import os
import wget
from ultralytics import YOLO


env_config = read_env('../ENV.txt')
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'
SENDING_METHOD = env_config['SENDING_METHOD']
ENABLE_DRIVE_UPLOAD = env_config['ENABLE_DRIVE_UPLOAD'] == 'True'
FIRE_RESULTS_FOLDER_DRIVE_ID = env_config['FIRE_RESULTS_FOLDER_DRIVE_ID']

MODEL_TYPE = env_config['FIRE_DETECT_MODEL'] # YOLO or VGG16

model = None

if MODEL_TYPE == 'VGG16':

    # check model file exists if not download with wget
    if not os.path.exists('model.h5'):
        print('MODEL FILE NOT FOUND, DOWNLOADING...')
        url = 'https://drive.google.com/uc?export=download&id=1rVO_T6Q7iNvCyEUoRBUuL7dC1tEPRZuW'
        wget.download(url, 'model.h5')
        print('MODEL FILE DOWNLOADED')

    # FIRE DETECT AI MODEL

    model = load_model('model.h5')

elif MODEL_TYPE == 'YOLO':

    model = YOLO("model.pt")

    model.predict('test.jpg')

# Connect Google Drive API

driveAPI = None
if ENABLE_DRIVE_UPLOAD:
    driveAPI = DriveAPI('../credentials.json')

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'fire_detect_server'
client_config['message.max.bytes'] = 32000000
client_config['fetch.message.max.bytes'] = 32000000


consumer = Consumer(client_config)

running = True
counter = 1

def prepare_data(image_data):
    img = np.array(image_data)
    img = img.reshape(1, img.shape[0], img.shape[1], img.shape[2])
    img = np.resize(img,(1, 224, 224, 3))
    return img

def predict_fire(image_path = None, image_data = None, msgKey = None):
    global counter
    if MODEL_TYPE == 'VGG16':

        # {'default': 0, 'fire': 1, 'smoke': 2}
        
        
        print('PREDICTING FIRE...')
        results = None
        if image_path:
            image_data = Image.open(image_path)
        
        results = model.predict(prepare_data(image_data))

        # turn into numpy array
        results = np.array(results)
        
        # {'default': 0, 'fire': 1, 'smoke': 2}
        labels = ['default', 'fire', 'smoke']
        prediction = labels[results.argmax()]
        print('FIRE PREDICTION: ', prediction)
           
        if ENABLE_DRIVE_UPLOAD:
            # send results to kafka
            value = json.dumps({'prediction': prediction, 'rawImageKey': msgKey})
            print('SENDING VALUE TO KAFKA: ', value)
            producer.produce('fireResults', key=msgKey, value=value)
        else:
            # check fire_detect_server folder in results folder
            if not os.path.exists('../results/fire_detect_server'):
                os.mkdir('../results/fire_detect_server')
            image_data.save('../results/fire_detect_server/{}_{}.jpg'.format(counter, prediction))
            # store path and prediction in value variable
            value = json.dumps({'prediction': prediction, 
                                'key' : 'fire_pred_' + str(counter) + '_' + prediction, 
                                'path': '../results/fire_detect_server/' + str(counter) + '_' + prediction + '.jpg'})
            # send results to kafka
            producer.produce('fireResults', key=msgKey, value=value)

        if SENDING_METHOD == 'flush':
            producer.flush()
        if SENDING_METHOD == 'poll':
            producer.poll(0)

        counter += 1
    
    elif MODEL_TYPE == 'YOLO':

        # fire : 0
        # others : 1
        # smoke : 2
        print('PREDICTING FIRE AND SMOKE...')
        results = None
        if image_path:
            results = model(image_path)
        if image_data:
            results = model(image_data)
        labels = {0: u'__background__', 1: u'fire', 2: u'others',3: u'smoke'}
        result_image_data = None
        if image_path:
            result_image_data = plot_results(results, folder_path='../results/fire_pred/', image_path=image_path, labels=labels, result_name = 'fire_pred_' + str(counter) + '.jpg', save_image=False, return_image=True)
        else:
            result_image_data = plot_results(results, folder_path='../results/fire_pred/' ,image_data=image_data, labels=labels, result_name = 'fire_pred_' + str(counter) + '.jpg', save_image=False, return_image=True)
        
        if(type(result_image_data) == np.ndarray):
            result_image_data = Image.fromarray(result_image_data)
        
       
        counter += 1
        print('PREDICTION', results[0].boxes.boxes)

        # save result_image_data to local file
        result_image_data.save('temp.jpg')
        
        if ENABLE_DRIVE_UPLOAD:
            # send result_image_data to google drive
            file_id = driveAPI.FileUpload('temp.jpg', 'fire_detect' + str(counter) + '.jpg', FIRE_RESULTS_FOLDER_DRIVE_ID)

            # SEND RESULTS TO KAFKA
            value_ = json.dumps({'file_id' : file_id, 'key' : 'fire_detect' + str(counter) + '.jpg'})
            producer.produce('fireResults', key=str(counter), value=value_)
        else:
            if not os.path.exists('../results/fire_detect'):
                os.mkdir('../results/fire_detect')

            result_image_data.save('../results/fire_detect/' + 'fire_pred_' + str(counter) + '.jpg')
            # store path and prediction in value variable
            value = json.dumps({'prediction': "", 'path': '../results/fire_detect/' + 'fire_pred_' + str(counter) + '.jpg',
                                'key' : 'fire_pred_' + str(counter)})
            # send results to kafka
            producer.produce('fireResults', key=msgKey, value=value)

        
        if SENDING_METHOD == 'flush':
            producer.flush()
        if SENDING_METHOD == 'poll':
            producer.poll(0)

        print('RESULT SENT TO KAFKA')



try:
    #if ENABLE_UPSAMPLING:
    #    consumer.subscribe(['upsampledPersonByte'])
    #else:
    #    consumer.subscribe(['croppedPersonByte'])
    consumer.subscribe(['rawImageByte'])
    print('SUBSCRIBED TO TOPIC: rawImageByte')
    print('FIRE DETECT SERVER STARTED')
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
            msg_json = json.loads(msg.value())
            print('MESSAGE RECEIVED IN FIRE DETECT SERVER', msg_json)
            if ENABLE_DRIVE_UPLOAD:
                predict_fire(image_data=getImageDataFromDriveFileId(driveAPI,msg_json['file_id']))
            else:
                predict_fire(image_path=msg_json['path'])

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
