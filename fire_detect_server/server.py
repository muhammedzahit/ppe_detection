import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer
import json
import numpy as np
from PIL import Image
from io import BytesIO
from utils import read_ccloud_config, get_image_data_from_bytes, read_env, plot_results, get_bytes_from_image_data
import tensorflow as tf
from tensorflow.keras.models import load_model
import os
import wget
from ultralytics import YOLO


env_config = read_env('../ENV.txt')
SAVE_RESULTS = env_config['AI_MODELS_SAVE_RESULTS'] == 'True'
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'
SENDING_METHOD = env_config['SENDING_METHOD']

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

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'fire_detect_server'

print('CLIENT CONFIG',client_config)
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
        print('RESULTS', results)
        
        # {'default': 0, 'fire': 1, 'smoke': 2}
        labels = ['default', 'fire', 'smoke']
        prediction = labels[results.argmax()]
        print('FIRE PREDICTION: ', prediction)
        if SAVE_RESULTS:
            # check fire_detect_server folder in results folder
            if not os.path.exists('../results/fire_detect_server'):
                os.mkdir('../results/fire_detect_server')
            image_data.save('../results/fire_detect_server/{}_{}.jpg'.format(counter, prediction))
        
        # send results to kafka
        value = json.dumps({'prediction': prediction, 'rawImageKey': msgKey})
        print('SENDING VALUE TO KAFKA: ', value)
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
        print('RES',results[0].boxes.boxes)
        result_image_data = None
        if image_path:
            result_image_data = plot_results(results, folder_path='../results/fire_pred/', image_path=image_path, labels=labels, result_name = 'fire_pred_' + str(counter) + '.jpg', save_image=False, return_image=True)
        else:
            result_image_data = plot_results(results, folder_path='../results/fire_pred/' ,image_data=image_data, labels=labels, result_name = 'fire_pred_' + str(counter) + '.jpg', save_image=False, return_image=True)
        if SAVE_RESULTS:
            if not os.path.exists('../results/fire_detect'):
                os.mkdir('../results/fire_detect')

            print('TYPE OF RESULT IMAGE DATA', type(result_image_data))

            if(type(result_image_data) == np.ndarray):
                result_image_data = Image.fromarray(result_image_data)

            result_image_data.save('../results/fire_detect/' + 'fire_pred_' + str(counter) + '.jpg')
        
        counter += 1
        print('PREDICTION', results[0].boxes.boxes)
        
        # SEND RESULTS TO KAFKA
        producer.produce('fireResults', key=str(counter), value=get_bytes_from_image_data(result_image_data))
        
        if SENDING_METHOD == 'flush':
            producer.flush()
        if SENDING_METHOD == 'poll':
            producer.poll(0)

        print('RESULT IMAGE SENT TO KAFKA')



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
            print('IMAGE RECEIVED')
            msgKey = msg.key().decode('utf-8')
            img = get_image_data_from_bytes(msg.value())
            predict_fire(image_data=img, msgKey=msgKey)

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
