# connect local flask server and get response
import requests
import sys
sys.path.append('../')
from utils import read_env, read_ccloud_config, get_image_data_from_bytes
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

client_config['group.id'] = 'database_rawImageByte'
consumer = Consumer(client_config)
consumer.subscribe(['rawImageByte'])

client_config['group.id'] = 'database_croppedPersonByte'
consumer_croppedPersonByte = Consumer(client_config)
consumer_croppedPersonByte.subscribe(['croppedPersonByte'])

client_config['group.id'] = 'database_upsampledPersonByte'
consumer_upsampledPersonByte = Consumer(client_config)
consumer_upsampledPersonByte.subscribe(['upsampledPersonByte'])

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
            

def thread_type_1(consumer, msg_type):
    print(msg_type + ' thread started')
    while True:
        msg = consumer.poll(timeout=1.0)
        if checkMessage(msg):
            #msg = msg.value().decode('utf-8')
            messageKey = msg.key().decode('utf-8')
            image_name = msg_type + '/' + messageKey
            saveMessage(msg_type, msg, image_path='images/' + image_name + '.jpg')

            if msg_type != 'rawImageByte':
                updateResults(msg_type, image_path=image_name, success=True, pred="")

def thread_type_2(consumer, msg_type, key, path):
    print(msg_type + ' thread started... // ', 'key : ', key, '// path:', path)
    KEY = key
    while True:
        msg = consumer.poll(timeout=1.0)
        if checkMessage(msg):
            msgValue = msg.value().decode('utf-8')
            msgValue = json.loads(msgValue)
            key = msgValue[KEY]
            prediction = msgValue['prediction']

            updateResults(msg_type, image_path=path+key, success=True, pred=prediction)


threading.Thread(target=thread_type_1, args=(consumer_hardhat, 'hardhat_results')).start()
threading.Thread(target=thread_type_1, args=(consumer, 'rawImageByte')).start()

personByteTopic = None
if ENABLE_UPSAMPLING:
    personByteTopic = {'consumer' : consumer_upsampledPersonByte, 'topic' : 'upsampledPersonByte'}
else:
    personByteTopic = {'consumer' : consumer_croppedPersonByte, 'topic' : 'croppedPersonByte'}

threading.Thread(target=thread_type_1, args=(personByteTopic['consumer'], personByteTopic['topic'])).start()

if FIRE_DETECT_MODEL == 'VGG16':
    threading.Thread(target=thread_type_2, args=(consumer_fire, 'fire_results', 'rawImageKey', 'rawImageByte/')).start()
elif FIRE_DETECT_MODEL == 'YOLO':
    threading.Thread(target=thread_type_1, args=(consumer_fire, 'fire_results')).start()

threading.Thread(target=thread_type_2, args=(consumer_smoker, 'smoker_results', 'croppedPersonKey', personByteTopic['topic'] + '/')).start()
threading.Thread(target=thread_type_2, args=(consumer_age, 'age_results', 'croppedPersonKey', personByteTopic['topic'] + '/')).start()
