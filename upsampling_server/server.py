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
from datetime import datetime

ANIME_4K = 'anime4k'
FSRCNN = 'fsrcnn'
FSRCNN_SCALE = 2
UPSAMPLING_MODE = ANIME_4K

#CONNECT TO KAFKA

client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)


# READ ENV
env_config = read_env('../ENV.txt')
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'
ENABLE_DRIVE_UPLOAD = env_config['ENABLE_DRIVE_UPLOAD'] == 'True'
UPSCALED_IMAGES_FOLDER_DRIVE_ID = env_config['UPSCALED_IMAGES_FOLDER_DRIVE_ID']

# Connect to Google Drive
driveAPI = None
if ENABLE_DRIVE_UPLOAD:
    driveAPI = DriveAPI('../credentials.json')


# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'upsampling_server'
client_config['message.max.bytes'] = 32000000
client_config['fetch.message.max.bytes'] = 32000000


consumer = Consumer(client_config)

running = ENABLE_UPSAMPLING
num = 0

# FSRCCN MODEL
fsrcnn_model = load_model(scale = FSRCNN_SCALE)
counter = 0

def upsample_image(image_data):
    global counter
    counter += 1
    output_file = None
    print('-------------------------------')
    print('---------------------------------------------')
    print('IMAGE UPSAMPLING STARTED', datetime.utcnow())
    print('---------------------------------------------')
    print('-------------------------------')
    if UPSAMPLING_MODE == ANIME_4K:
        image_data.save('temp.jpg')
        pyanime4k.upscale_images(pathlib.Path('temp.jpg'))
        output_file = 'temp_output.jpg'
        
    elif UPSAMPLING_MODE == FSRCNN:
        image_data.save('temp.jpg')
        upscale_image(image_path = 'temp.jpg', scale = FSRCNN_SCALE, model = fsrcnn_model)
        output_file = 'sr.png'

    if ENABLE_DRIVE_UPLOAD:
        # save output file to drive
        file_id = driveAPI.FileUpload(filepath=output_file, name='upsampled_' + str(counter) + '.jpg', folder_id=UPSCALED_IMAGES_FOLDER_DRIVE_ID)
        print('FILE SAVED TO DRIVE', file_id)
        
        value_ = json.dumps({'file_id' : file_id, 'key' : 'upsampled' + str(counter) + '.jpg'})

        producer.produce('upsampledPersonByte', key = "upsampled" + str(counter), value = value_)
    else:
        # copy output file to results folder
        # and check upsampled folder exists in results folder
        pathlib.Path('../results/upsampled').mkdir(parents=True, exist_ok=True)
        pathlib.Path('../results/upsampled').joinpath('upsampled_' + str(counter) + '.jpg').write_bytes(open(output_file, 'rb').read())
        value_ = json.dumps({'path' : '../results/upsampled/upsampled_' + str(counter) + '.jpg', 'key' : 'upsampled' + str(counter) + '.jpg'})
        producer.produce('upsampledPersonByte', key = "upsampled" + str(counter), value = value_)

    print('---------------------------------')
    print('IMAGE UPSAMPLING FINISHED', datetime.utcnow())
    print('---------------------------------')
    producer.flush()



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
            print('MESSAGE RECEIVED IN UPSAMPLING SERVER : ', msg_json)
            if ENABLE_DRIVE_UPLOAD:
                upsample_image(image_data = getImageDataFromDriveFileId(driveAPI, msg_json['file_id']))
            else:
                upsample_image(image_data=Image.open(msg_json['path']))

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()


