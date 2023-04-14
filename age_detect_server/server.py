import sys
sys.path.append('../')
from confluent_kafka import Consumer
import json
import numpy as np
import cv2
from PIL import Image
from io import BytesIO
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, plot_results, read_env
import tensorflow as tf
from tensorflow.keras.preprocessing.image import load_img


env_config = read_env('../ENV.txt')
SAVE_RESULTS = True
ENABLE_UPSAMPLING = False if env_config['ENABLE_UPSAMPLING'] == 'False' else True

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'age_detect_server'

print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)

running = True
num = 0

# HARDHAT DETECT AI MODEL

model = tf.keras.models.load_model('model.h5')
counter = 0


def predict_age(image_path = None, image_data = None):
    global counter
    print('PREDICTING AGE...')
    results = None
    print('RES',results[0].boxes.boxes)

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
    for (x,y,w,h) in faces:
        cv2.rectangle(img,(x,y),(x+w,y+h),(255,0,0),2)
        roi_color = img[y:y+h, x:x+w]
        cv2.imwrite('faces/face' + str(count) + '.jpg', roi_color)
        count += 1
        
    for i in range(count):
        img = load_img("faces/face" + str(i) + ".jpg", grayscale=True)
        img = img.resize((128,128), Image.ANTIALIAS)
        img = np.array(img)
        img = img / 255
        pred = model.predict(img.reshape(1, 128, 128, 1))
        pred_gender = gender_dict[round(pred[0][0][0])] 
        pred_age = round(pred[1][0][0])
        print("Prediction: Gender = ", pred_gender," Age = ", pred_age)


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
            predict_age(image_data=img)

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
