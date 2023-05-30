import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import json
import numpy as np
import cv2
from PIL import Image
from utils import read_ccloud_config, get_image_data_from_bytes, read_env, DriveAPI, getDriveDownloadLink, downloadImageFromURL\
, getImageDataFromDriveFileId
import tensorflow as tf
import wget
import os
import patoolib
import shutil


env_config = read_env('../ENV.txt')
ENABLE_DRIVE_UPLOAD = env_config['ENABLE_DRIVE_UPLOAD'] == 'True'
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'
SENDING_METHOD = env_config['SENDING_METHOD']

if not os.path.exists('./model'):
    os.makedirs('./model')

# check model file exists unzip model.zip 
if not os.path.exists('./model/model.h5'):
    # check rar files exists in model folder
    # if not download rar files
    if not os.path.exists('./model/model.part1.rar'):
        urls = {
            'model.part1.rar': 'https://drive.google.com/uc?export=download&id=1LPG-X9kJ97m6VzMX1fgMIShLlKRQGP50',
            'model.part2.rar': 'https://drive.google.com/uc?export=download&id=1WQr58waH28OWOPzg_cFGU0zEZ7Ctht43',
            'model.part3.rar': 'https://drive.google.com/uc?export=download&id=1TpUVZK_dje9lbZblgoJoShR0VIKPo7jZ',
            'model.part4.rar': 'https://drive.google.com/uc?export=download&id=1Hje4OyL72tdEjcp9I3H0cfi32b9FCt9r'
        }
        for file_name, url in urls.items():
            print('DOWNLOADING', file_name)
            wget.download(url, './model/'+file_name)
            print('DOWNLOADED', file_name)
    print('UNZIPPING...')
    patoolib.extract_archive('./model/model.part1.rar', outdir='./model/')

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# Connect to Google Drive API
driveAPI = None
if ENABLE_DRIVE_UPLOAD:
    driveAPI = DriveAPI('../credentials.json')

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'age_detect_server'
client_config['message.max.bytes'] = 32000000
client_config['fetch.message.max.bytes'] = 32000000

consumer = Consumer(client_config)

running = True
num = 0

# AGE DETECT AI MODEL

model = tf.keras.models.load_model('./model/model.h5')
counter = 0


def predict_age(parent_image_id,image_path = None, image_data = None, msgKey = None):
    global counter
    print('PREDICTING AGE...')
    results = None
    
    if image_path is not None: 
        image_data = cv2.imread(image_path)
    
    image_data = np.array(image_data)
    #print('RES',results[0].boxes.boxes)

    face_cascade = cv2.CascadeClassifier('cascade/haarcascade_frontalface_default.xml')
    gender_dict = {0:"Male",1:"Female"}
    img = image_data
    imgGray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(        
                imgGray,
                scaleFactor=1.3,
                minNeighbors=3,
                minSize=(30, 30)
            )

    count = 0
    max_age = 0
    print("Number of faces detected: " + str(len(faces)))

    if(len(faces) == 0):
        max_age = 'NO FACE'
    else:
        for (x,y,w,h) in faces:
            cv2.rectangle(img,(x,y),(x+w,y+h),(255,0,0),2)
            roi_color = img[y:y+h, x:x+w]
            cv2.imwrite('faces/face' + str(count) + '.jpg', roi_color)
            count += 1
            
        for i in range(count):
            img = tf.keras.preprocessing.image.load_img("faces/face" + str(i) + ".jpg", color_mode = "grayscale")
            img = img.resize((128,128), Image.LANCZOS)
            img = np.array(img)
            img = img / 255
            pred = model.predict(img.reshape(1, 128, 128, 1))
            pred_gender = gender_dict[round(pred[0][0][0])] 
            pred_age = round(pred[1][0][0])
            max_age = max(max_age, pred_age)
            print("Prediction: Gender = ", pred_gender," Age = ", pred_age)

    
    if ENABLE_DRIVE_UPLOAD:
        # send result to kafka
        value = json.dumps({'prediction': max_age, 'file_id': msgKey, 
                            'key' : 'age_detect_server_' + str(counter) + '.jpg'
                            ,'parent_image_id' : parent_image_id})
        print('SENDING VALUE TO KAFKA: ', value)
        producer.produce('ageResults', key=msgKey, value=value)
    else:
        #check age_detect_server folder in results folder
        if not os.path.exists('../results/age_detect_server'):
            os.makedirs('../results/age_detect_server')
        shutil.copyfile(image_path, '../results/age_detect_server/age_pred_' + str(counter)  + '-Age ' + str(max_age) + '.jpg')
        
        value = json.dumps({'prediction': max_age, 
                            'key' : 'age_pred_' + str(counter)  + '-Age ' + str(max_age) + '.jpg',
                            'path' : '../results/age_detect_server/age_pred_' + str(counter)  + '-Age ' + str(max_age) + '.jpg',
                            'parent_image_id' : parent_image_id})
        print('SENDING VALUE TO KAFKA: ', value)
        producer.produce('ageResults', key=msgKey, value=value)

    counter += 1

    if SENDING_METHOD == 'flush':
        producer.flush()
    if SENDING_METHOD == 'poll':
        producer.poll(0)
    
    
    counter += 1
    
    print('--------------------------------')


try:
    if ENABLE_UPSAMPLING:
        consumer.subscribe(['upsampledPersonByte'])
        print('SUBSCRIBED TO TOPIC: upsampledPersonByte')
    else:
        consumer.subscribe(['croppedPersonByte'])
        print('SUBSCRIBED TO TOPIC: croppedPersonByte')
    print('AGE DETECT SERVER STARTED')
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
            print('MESSAGE RECEIVED IN AGE DETECTION SERVER : ', msg_json)

            if ENABLE_DRIVE_UPLOAD:
                predict_age(parent_image_id=msg_json['file_id'],image_data= getImageDataFromDriveFileId(driveAPI,msg_json['file_id']), msgKey = msg_json['file_id'])
            else:
                predict_age(parent_image_id=msg_json['path'],image_path = msg_json['path'], msgKey = str(counter))
            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()