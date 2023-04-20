import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import json
import numpy as np
import cv2
from PIL import Image
from io import BytesIO
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, plot_results, read_env
import tensorflow as tf
import wget
import os
import patoolib
import shutil


env_config = read_env('../ENV.txt')
SAVE_RESULTS = False if env_config['AI_MODELS_SAVE_RESULTS'] == 'False' else False
ENABLE_UPSAMPLING = False if env_config['ENABLE_UPSAMPLING'] == 'False' else True
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

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'age_detect_server'

print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)

running = True
num = 0

# AGE DETECT AI MODEL

model = tf.keras.models.load_model('./model/model.h5')
counter = 0


def predict_age(image_path = None, image_data = None, msgKey = None):
    global counter
    print('PREDICTING AGE...')
    results = None
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
    min_age = 100
    print("Number of faces detected: " + str(len(faces)))

    if(len(faces) == 0):
        print('NO FACE DETECTED EXITING...')
        return

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
        min_age = min(min_age, pred_age)
        print("Prediction: Gender = ", pred_gender," Age = ", pred_age)

        if SAVE_RESULTS:
            # check age_detect_server folder in results folder
            if not os.path.exists('../results/age_detect_server'):
                os.makedirs('../results/age_detect_server')
            #os.popen("cp faces/face" + str(i) + ".jpg" + '../results/age_detect_server/age_detect_server_' + str(counter)  + '-gender: ' + pred_gender + '-age:' + str(pred_age) + '.jpg')
            shutil.copyfile("faces/face" + str(i) + ".jpg", '../results/age_detect_server/age_detect_server_' + str(counter)  + '-Gender ' + pred_gender + '-Age ' + str(pred_age) + '.jpg')
            #cv2.imwrite('../results/age_detect_server/age_detect_server_' + str(counter)  + '-gender:' + pred_gender + '-age:' + str(pred_age) + '.jpg', img2)
            counter += 1
        
    # send result to kafka
    value = json.dumps({'prediction': min_age, 'croppedPersonKey': msgKey})
    print('SENDING VALUE TO KAFKA: ', value)
    producer.produce('ageResults', key=msgKey, value=value)

    if SENDING_METHOD == 'flush':
        producer.flush()
    if SENDING_METHOD == 'poll':
        producer.poll(0)
    
    
    counter += 1
    
    print('--------------------------------')


try:
    if ENABLE_UPSAMPLING:
        consumer.subscribe(['upsampledPersonByte'])
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
            print('IMAGE RECEIVED')
            img = get_image_data_from_bytes(msg.value())
            msgKey = msg.key().decode('utf-8')
            predict_age(image_data=img, msgKey = msgKey)

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
