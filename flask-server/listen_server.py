# connect local flask server and get response
import requests
import sys
sys.path.append('../')
from utils import read_env, read_ccloud_config, get_image_data_from_bytes, getDriveDownloadLink, DriveAPI
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import os
import json
import threading

env_config = read_env('../ENV.txt')

ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'

FIRE_DETECT_MODEL = env_config['FIRE_DETECT_MODEL']

# connect to local flask server
url = env_config['FLASK_SERVER_URL']
image_dict = {}
COUNTER = 0

# check if images folder exists
if not os.path.exists('images'):
    os.makedirs('images')

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
print('CLIENT CONFIG',client_config)
producer = Producer(client_config)

client_config['group.id'] = 'hardhatResults'
consumer_hardhat = Consumer(client_config)
consumer_hardhat.subscribe(['hardhatResults'])

client_config['group.id'] = 'fireResults'
consumer_fire = Consumer(client_config)
consumer_fire.subscribe(['fireResults'])

client_config['group.id'] = 'ageResults'
consumer_age = Consumer(client_config)
consumer_age.subscribe(['ageResults'])

client_config['group.id'] = 'smokerResults'
consumer_smoker = Consumer(client_config)
consumer_smoker.subscribe(['smokerResults'])

client_config['group.id'] = 'streamResults'
consumer_stream = Consumer(client_config)
consumer_stream.subscribe(['streamResults'])

driveAPI = DriveAPI('../credentials.json')

def checkMessage(msg):
    if msg is None: 
        return False
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
            (msg.topic(), msg.partition(), msg.offset()))
        elif msg.error():
            raise KafkaException(msg.error())
    return True

def updateResults(type, image_link, success, pred="", key=""):
    print(url + 'updateResults')
    r = requests.post(url + 'updateResults', json={'type': type, 'image_link': image_link, 'success': success, 'pred': pred, 'key' : key})
    print(r.json())
            
            

def thread_type(consumer, consumer_type):
    print('THREAD STARTED', consumer_type)
    while True:
        msg = consumer.poll(timeout=1.0)
        if checkMessage(msg):
            #msg = msg.value().decode('utf-8')
            msg_json = json.loads(msg.value().decode('utf-8'))
            print('MESSAGE RECEIVED : ', msg_json)

            image_link = getDriveDownloadLink(msg_json['file_id'])

            pred=""
            if 'prediction' in msg_json:
                pred = msg_json['prediction']

            success=True
            if 'success' in msg_json:
                success = msg_json['success']

            updateResults(consumer_type, image_link=image_link, success=success, pred=pred, key=msg_json['key'])

def thread_type_2(consumer, consumer_type):
    print('THREAD STARTED', consumer_type)
    while True:
        msg = consumer.poll(timeout=1.0)
        if checkMessage(msg):
            #msg = msg.value().decode('utf-8')
            msg_json = json.loads(msg.value().decode('utf-8'))
            print('MESSAGE RECEIVED : ', msg_json)
            
            file_id = driveAPI.FileUpload(msg_json['path'], msg_json['key'], folder_id='17noLHuDVES1My9knrGuQAgLVjaTqX-Qq')

            producer.produce(topic='rawImageByte', value=json.dumps({'file_id': file_id, 'key': msg_json['key']}))
            producer.flush()


threading.Thread(target=thread_type, args=(consumer_age, 'age_results')).start()
threading.Thread(target=thread_type, args=(consumer_smoker, 'smoker_results')).start()
threading.Thread(target=thread_type, args=(consumer_fire, 'fire_results')).start()
threading.Thread(target=thread_type, args=(consumer_hardhat, 'hardhat_results')).start()
threading.Thread(target=thread_type_2, args=(consumer_stream, 'stream_results')).start()
