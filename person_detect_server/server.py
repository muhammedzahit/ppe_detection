import sys
sys.path.append('../')
from confluent_kafka import Consumer,Producer
from PIL import Image
from ultralytics import YOLO
from utils import getImageDataFromDriveFileId, read_ccloud_config, get_bytes_from_image_data, \
    get_image_data_from_bytes, plot_results, read_env, DriveAPI, getDriveDownloadLink, downloadImageFromURL
import sys
import json
import os

PERSON_DETECT_THRESHOLD = 0.67

# CONNECT TO KAFKA

env_config = read_env('../ENV.txt')

SENDING_METHOD = env_config['SENDING_METHOD']
ENABLE_DRIVE_UPLOAD = env_config['ENABLE_DRIVE_UPLOAD'] == 'True'
CROPPED_PERSONS_FOLDER_DRIVE_ID = env_config['CROPPED_PERSONS_FOLDER_DRIVE_ID']
COUNTER = 0

# CONNECT TO GOOGLE DRIVE API
driveAPI = None
if ENABLE_DRIVE_UPLOAD:
    driveAPI = DriveAPI('../credentials.json')

client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

client_config['group.id'] = 'foo'
client_config['message.max.bytes'] = 32000000
client_config['fetch.message.max.bytes'] = 32000000
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

    for p in person_imgs:
        COUNTER += 1
        if ENABLE_DRIVE_UPLOAD:
            p.save('upload.jpg')

            file_id = driveAPI.FileUpload('upload.jpg', name = 'person_detect' + str(COUNTER) + '.jpg', folder_id=CROPPED_PERSONS_FOLDER_DRIVE_ID)
            
            value_ = json.dumps({'file_id': file_id, 'key' : 'person_detect' + str(COUNTER) + '.jpg'})

            producer.produce('croppedPersonByte', key = "person" + str(COUNTER), value = value_)
        else:
            # check if person_detect folder exists in results folder
            if not os.path.exists('../results/person_detect'):
                os.makedirs('../results/person_detect')
            p.save('../results/person_detect/person_detect' + str(COUNTER) + '.jpg')
            value_ = json.dumps({'key' : 'person_detect' + str(COUNTER) + '.jpg', 'path' : '../results/person_detect/person_detect' + str(COUNTER) + '.jpg'})
            producer.produce('croppedPersonByte', key = "person" + str(COUNTER), value = value_)

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
        print('PERSON SIZES', i)
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
            msg_json = json.loads(msg.value().decode('utf-8'))
            print('MESSAGE RECEIVED IN PERSON DETECT SERVER : ',msg_json)

            if ENABLE_DRIVE_UPLOAD:
                predict_person(image_data = getImageDataFromDriveFileId(driveAPI,msg_json['file_id']))
            else:
                predict_person(image_path=msg_json['path'])
finally:
    # Close down consumer to commit final offsets.
    consumer.close()


