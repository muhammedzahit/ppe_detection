import sys
sys.path.append('../')
from confluent_kafka import Consumer,Producer
import json
import numpy as np
from PIL import Image
from io import BytesIO
from ultralytics import YOLO
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, plot_results, read_env
import sys

PERSON_DETECT_THRESHOLD = 0.6

# CONNECT TO KAFKA

env_config = read_env('../ENV.txt')

SENDING_METHOD = env_config['SENDING_METHOD']
SAVE_RESULTS = False if env_config['AI_MODELS_SAVE_RESULTS'] == 'False' else False
COUNTER = 0

client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

del client_config['message.max.bytes']

client_config['group.id'] = 'foo'
print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)


# PERSON DETECT AI MODEL

model = YOLO("yolov8n.pt")
model('./test.png')

print('---------------------------------')
print('PERSON DETECT SERVER STARTING ....')
print('---------------------------------')

def predict_person(image_path = None, image_data = None):
    global COUNTER
    print('PREDICTING PERSONS...')
    results, person_imgs = None, None
    if image_path:
        results = model(image_path)
        person_imgs = get_person_datas(results,image_path)
    if image_data:
        results = model(image_data)
        person_imgs = get_person_datas(results,image_data=image_data)
    print(len(person_imgs),' PERSON FOUND')

    print('SENDING DATA TO CROPPED_PERSON_BYTE TOPIC')
    
    COUNTER += 1
    if SAVE_RESULTS:
        plot_results(results, folder_path='../results/person_detect/',image_data=image_data, result_name = 'person_pred_' + str(COUNTER) + '.jpg')

    for p in person_imgs:
        byteImg = get_bytes_from_image_data(p)
        print('SENDING MESSAGE SIZE', len(byteImg), type(byteImg))
        producer.produce('croppedPersonByte', key = "person" + str(COUNTER), value = byteImg)
        
        if SENDING_METHOD == 'flush':
            producer.flush()
        if SENDING_METHOD == 'poll':
            producer.poll(0)
    
    

    print('---------------------------------')
    

def get_person_datas(results,image_path=None, image_data=None):
    img = image_data
    if image_path:
        img = Image.open(image_path)
    person_imgs = []
    
    for i in results[0].boxes.boxes:
        if(i[-1] == 0 and i[-2] > PERSON_DETECT_THRESHOLD): # class 0 ise / Person tahmin edildiyse ve threshold uzerindeyse
            i0, i1, i2, i3 = int(i[0]), int(i[1]),int(i[2]),int(i[3])
            crop = img.crop((i0,i1,i2,i3))
            person_imgs.append(img.crop((i0,i1,i2,i3)))
    return person_imgs


running = True

try:
    consumer.subscribe(['rawImageByte'])

    print('CONNECTING TO RAW_IMAGE_BYTE TOPIC ....')
    print('---------------------------------')

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None: 
            continue
        if msg.error():
            raise Exception('MSG ERROR')
        else:
            #msg = msg.value().decode('utf-8')
            print(msg.key())
            print('IMAGE RECEIVED') 
            img = get_image_data_from_bytes(msg.value())
            

            # Tahmin Et - Predict

            predict_person(image_data = img)
finally:
    # Close down consumer to commit final offsets.
    consumer.close()


