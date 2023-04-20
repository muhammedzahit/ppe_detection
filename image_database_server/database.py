# connect local flask server and get response
import requests
import sys
sys.path.append('../')
from utils import read_env, read_ccloud_config, get_image_data_from_bytes, get_bytes_from_image_data
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import os
import json
import pickle
import urllib3

env_config = read_env('../ENV.txt')

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

client_config['group.id'] = 'database_rawImageByte'
consumer = Consumer(client_config)
consumer.subscribe(['rawImageByte'])

client_config['group.id'] = 'database_croppedPersonByte'
consumer_croppedPersonByte = Consumer(client_config)
consumer_croppedPersonByte.subscribe(['croppedPersonByte'])

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
        
def saveMessage(folder, msg, image_path):
    # check if folder exists in images folder
    if not os.path.exists('images/' + folder):
        os.makedirs('images/' + folder)
    # save image to folder
    image_data = get_image_data_from_bytes(msg.value())
    if image_data.mode != 'RGB':
        image_data = image_data.convert('RGB')
    image_data.save(image_path)

def updateResults(type, image_path, success, pred=""):
    print(url + 'updateResults')
    r = requests.post(url + 'updateResults', json={'type': type, 'image_path': image_path, 'success': success, 'pred': pred})
    print(r.json())
            

print('WAITING FOR UPLOADED IMAGE')
while True:
    msgRawImageByte = consumer.poll(timeout=1.0)
    msgHardhat = consumer_hardhat.poll(timeout=1.0)
    msgFire = consumer_fire.poll(timeout=1.0)
    msgCroppedPersonByte = consumer_croppedPersonByte.poll(timeout=1.0)
    msgSmoker = consumer_smoker.poll(timeout=1.0)
    msgAge = consumer_age.poll(timeout=1.0)

    messages = {'hardhat_results' : msgHardhat, 'rawImageByte' : msgRawImageByte, 'croppedPersonByte' : msgCroppedPersonByte,
                'smoker_results' : msgSmoker, 'fire_results' : msgFire, 'age_results' : msgAge}
    
    for msg_type in ['hardhat_results', 'rawImageByte', 'croppedPersonByte']:
        msg = messages[msg_type]
        if checkMessage(msg):
            #msg = msg.value().decode('utf-8')
            messageKey = msg.key().decode('utf-8')
            image_name = msg_type + '/' + messageKey
            saveMessage(msg_type, msg, image_path='images/' + image_name + '.jpg')

            if msg_type != 'rawImageByte':
                updateResults(msg_type, image_path=image_name, success=True, pred="")

    keys = {'fire_results' : 'rawImageKey', 'smoker_results' : 'croppedPersonKey', 'age_results' : 'croppedPersonKey'}
    paths = {'fire_results' : 'rawImageByte/', 'smoker_results' : 'croppedPersonByte/', 'age_results' : 'croppedPersonByte/'}

    for msg_type in ['smoker_results', 'fire_results', 'age_results']:
        msg = messages[msg_type]
        if checkMessage(msg):
            msgValue = msg.value().decode('utf-8')
            msgValue = json.loads(msgValue)
            print(msgValue)
            key = msgValue[keys[msg_type]]
            prediction = msgValue['prediction']

            updateResults(msg_type, image_path=paths[msg_type]+key, success=True, pred=prediction)

    