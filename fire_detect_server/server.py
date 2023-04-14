import sys
sys.path.append('../')
from confluent_kafka import Consumer
import json
import numpy as np
from PIL import Image
from io import BytesIO
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, plot_results, read_env
import tensorflow as tf
from tensorflow.keras.models import load_model
import os


env_config = read_env('../ENV.txt')
SAVE_RESULTS = True
ENABLE_UPSAMPLING = False if env_config['ENABLE_UPSAMPLING'] == 'False' else True

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'fire_detect_server'

print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)

running = True
counter = 1

# FIRE DETECT AI MODEL

model = load_model('model.h5')

def prepare_data(image_data):
    img = np.array(image_data)
    img = img.reshape(1, img.shape[0], img.shape[1], img.shape[2])
    img = np.resize(img,(1, 224, 224, 3))
    return img

def predict_fire(image_path = None, image_data = None):
    # {'default': 0, 'fire': 1, 'smoke': 2}
    
    global counter
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

    counter += 1



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
            img = get_image_data_from_bytes(msg.value())
            predict_fire(image_data=img)

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
