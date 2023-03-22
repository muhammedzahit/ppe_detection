import sys
sys.path.append('../')
from confluent_kafka import Consumer
import json
import numpy as np
from PIL import Image
from io import BytesIO
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, plot_results
from ultralytics import YOLO
from model import return_model_and_generator, predict_cigaratte_smoker

SAVE_RESULTS = True
ENABLE_UPSAMPLING = True

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'cigaratte_detect_server'

print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)

running = True
num = 0

# CIGARATTE DETECT AI MODEL

model,gen = return_model_and_generator()
counter = 0


def predict_smoker(image_path = None, image_data = None):
    # helmet : 0
    # vest : 1
    # head : 2
    global counter
    print('CHECKING CIGARATTE SMOKER...')
    
    prediction = None
    if image_path:
        prediction = predict_cigaratte_smoker(model, gen, image_path)
    if image_data:
        image_data.save('temp.jpg')
        prediction = predict_cigaratte_smoker(model, gen, 'temp.jpg')

    print('--'*40)
    print('PREDICTION: ', prediction)
    print('--'*40)


try:
    if ENABLE_UPSAMPLING:
        consumer.subscribe(['upsampledPersonByte'])
    else:
        consumer.subscribe(['croppedPersonByte'])
    print('SUBSCRIBED TO TOPIC: upsampledPersonByte')
    print('CIGARATTE DETECT SERVER STARTED')
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
            predict_smoker(image_data=img)

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()


