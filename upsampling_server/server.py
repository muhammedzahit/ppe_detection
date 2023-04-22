import pathlib
import pyanime4k
import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from PIL import Image
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, read_env
import datetime
from fsrcnn import load_model, upscale_image

ANIME_4K = 'anime4k'
FSRCNN = 'fsrcnn'
FSRCNN_SCALE = 4
UPSAMPLING_MODE = FSRCNN

#CONNECT TO KAFKA

client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# READ ENV
env_config = read_env('../ENV.txt')
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING']

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
    if UPSAMPLING_MODE == ANIME_4K:
        image_data.save('temp.jpg')
        pyanime4k.upscale_images(pathlib.Path('temp.jpg'))
        producer.produce('upsampledPersonByte', key = "upsampled" + str(counter), value = get_bytes_from_image_data(Image.open('temp_output.jpg')))
        producer.flush()
        print(str(datetime.datetime.now()),'IMAGE SENT TO UPSAMPLED_IMAGE_BYTE TOPIC')
        print('---------------------------------')
    elif UPSAMPLING_MODE == FSRCNN:
        image_data.save('temp.jpg')
        upscale_image(image_path = 'temp.jpg', scale = FSRCNN_SCALE, model = fsrcnn_model)
        producer.produce('upsampledPersonByte', key = "upsampled" + str(counter), value = get_bytes_from_image_data(Image.open('sr.png')))
        producer.flush()
        print(str(datetime.datetime.now()),'IMAGE SENT TO UPSAMPLED_IMAGE_BYTE TOPIC')
        print('---------------------------------')



try:
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
            print(str(datetime.datetime.now()) ,'IMAGE RECEIVED')
            img = get_image_data_from_bytes(msg.value())
            upsample_image(image_data = img)

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()


