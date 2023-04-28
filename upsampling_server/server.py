import json
import pathlib
import pyanime4k
import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from PIL import Image
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, read_env, DriveAPI, \
    getImageDataFromDriveFileId
import datetime
from fsrcnn import load_model, upscale_image

ANIME_4K = 'anime4k'
FSRCNN = 'fsrcnn'
FSRCNN_SCALE = 4
UPSAMPLING_MODE = FSRCNN

#CONNECT TO KAFKA

client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# Connect to Google Drive
driveAPI = DriveAPI('../credentials.json')

# READ ENV
env_config = read_env('../ENV.txt')
SAVE_RESULTS = env_config['AI_MODELS_SAVE_RESULTS'] == 'True'
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'

del client_config['message.max.bytes']

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'upsampling_server'

print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)

running = ENABLE_UPSAMPLING
num = 0

# FSRCCN MODEL
fsrcnn_model = load_model(scale = FSRCNN_SCALE)

counter = 0
SAVE_RESULTS = True

def upsample_image(image_data):
    global counter
    counter += 1
    output_file = None
    if UPSAMPLING_MODE == ANIME_4K:
        image_data.save('temp.jpg')
        pyanime4k.upscale_images(pathlib.Path('temp.jpg'))
        output_file = 'temp_output.png'
        
    elif UPSAMPLING_MODE == FSRCNN:
        image_data.save('temp.jpg')
        upscale_image(image_path = 'temp.jpg', scale = FSRCNN_SCALE, model = fsrcnn_model)
        output_file = 'sr.png'

    # save output file to drive
    file_id = driveAPI.FileUpload(filepath=output_file, name='upsampled_' + str(counter) + '.jpg', folder_id='12-WJ1nicQU5DEmpdZohADS-Xm8P9Cy2H')
    print('FILE SAVED TO DRIVE', file_id)
    
    value_ = json.dumps({'file_id' : file_id, 'key' : 'upsampled' + str(counter) + '.jpg'})

    producer.produce('upsampledPersonByte', key = "upsampled" + str(counter), value = value_)
    producer.flush()
    print(str(datetime.datetime.now()),'IMAGE SENT TO UPSAMPLED_IMAGE_BYTE TOPIC')
    print('---------------------------------')



try:
    if running:
        consumer.subscribe(['croppedPersonByte'])
        print('SUBSCRIBED TO TOPIC: croppedPersonByte')
        print('UPSAMPLING SERVER STARTED')
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
            print('MESSAGE RECEIVED', msg_json)
            upsample_image(image_data = getImageDataFromDriveFileId(driveAPI, msg_json['file_id']))

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()


