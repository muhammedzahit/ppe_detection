import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer
import json
from utils import read_ccloud_config, get_image_data_from_bytes, read_env, DriveAPI, getDriveDownloadLink, downloadImageFromURL\
, getImageDataFromDriveFileId
import mobilenetv2_model
import efficientnetb3_model
import os
import tensorflow as tf
import shutil

env_config = read_env('../ENV.txt')
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'
SENDING_METHOD = env_config['SENDING_METHOD']
ENABLE_DRIVE_UPLOAD = env_config['ENABLE_DRIVE_UPLOAD'] == 'True'
MODEL = 'EFFICIENTNETB3' # 'MOBILENETV2' # 'EFFICIENTNETB3'

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# Connect to Google Drive API
driveAPI = None
if ENABLE_DRIVE_UPLOAD:
    driveAPI = DriveAPI('../credentials.json')

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'cigaratte_detect_server'
client_config['message.max.bytes'] = 32000000
client_config['fetch.message.max.bytes'] = 32000000


consumer = Consumer(client_config)

running = True
num = 0
model = None

# bir üst klasöre cigaratte_detect adında bir klasör oluşturur
if not os.path.exists('../results/cigaratte_detect'):
    os.makedirs('../results/cigaratte_detect')

# CIGARATTE DETECT AI MODEL
if MODEL == 'MOBILENETV2':
    model,gen = mobilenetv2_model.return_model_and_generator()
elif MODEL == 'EFFICIENTNETB3':
    model = efficientnetb3_model.load_model()
counter = 0



def predict_smoker(parent_image_id = None,image_path = None, image_data = None, msgKey = None):
    global counter
    print('CHECKING CIGARATTE SMOKER...')
    
    if image_data:
        image_data.save('test.jpg')
        image_path = 'test.jpg' 

    prediction = None

    if MODEL == 'MOBILENETV2':
        prediction = mobilenetv2_model.predict_cigaratte_smoker(model, gen, image_path)
    elif MODEL == 'EFFICIENTNETB3':
        prediction = efficientnetb3_model.predict(model, image_path)
    counter += 1

    
    if ENABLE_DRIVE_UPLOAD:
        # send results to kafka
        value = json.dumps({'prediction': prediction, 
                            'key': 'cigaratte_pred_' + str(counter) + '.jpg', 
                            'file_id': msgKey,
                            'parent_image_id' : parent_image_id})
        print('SENDING VALUE TO KAFKA: ', value)
        producer.produce('smokerResults', key=msgKey, value=value)
    else:
        if image_data:
            image_data.save('../results/cigaratte_detect/cigaratte_pred_' + str(counter) + '_' + prediction + '.jpg')
        if image_path:
            # copy image to results folder with shutil
            shutil.copy(image_path, '../results/cigaratte_detect/cigaratte_pred_' + str(counter) + '_' + prediction + '.jpg')
        value = json.dumps({'prediction': prediction, 
                            'key': 'cigaratte_pred_' + str(counter) + '.jpg', 
                            'path' : '../results/cigaratte_detect/cigaratte_pred_' + str(counter) + '_' + prediction + '.jpg', 'file_id': msgKey
                            ,'parent_image_id' : parent_image_id})
        print('SENDING VALUE TO KAFKA: ', value)
        producer.produce('smokerResults', key=msgKey, value=value)
    
    if SENDING_METHOD == 'flush':
        producer.flush()
    if SENDING_METHOD == 'poll':
        producer.poll(0)

    counter += 1
    

    print('--'*40)
    print('PREDICTION: ', prediction)
    print('--'*40)


try:
    if ENABLE_UPSAMPLING:
        consumer.subscribe(['upsampledPersonByte'])
        print('SUBSCRIBED TO TOPIC: upsampledPersonByte')
    else:
        consumer.subscribe(['croppedPersonByte'])
        print('SUBSCRIBED TO TOPIC: croppedPersonByte')
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
            msg_json = json.loads(msg.value().decode('utf-8'))
            print('MESSAGE RECEIVED IN CIGARETTE DETECT SERVER : ', msg_json)

            if ENABLE_DRIVE_UPLOAD:
                predict_smoker(parent_image_id=msg_json['file_id'],image_data=getImageDataFromDriveFileId(driveAPI,msg_json['file_id']), msgKey=msg_json['file_id'])
            else:
                predict_smoker(parent_image_id=msg_json['path'],image_path=msg_json['path'], msgKey=msg_json['key'])
            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()


