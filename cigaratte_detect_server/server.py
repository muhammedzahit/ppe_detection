import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer
import json
from utils import read_ccloud_config, get_image_data_from_bytes, read_env
import mobilenetv2_model
import efficientnetb3_model
import os
import tensorflow as tf

env_config = read_env('../ENV.txt')
SAVE_RESULTS = env_config['AI_MODELS_SAVE_RESULTS'] == 'True'
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'
SENDING_METHOD = env_config['SENDING_METHOD']
MODEL = 'EFFICIENTNETB3' # 'MOBILENETV2' # 'EFFICIENTNETB3'

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'cigaratte_detect_server'

print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)

running = True
num = 0
model = None

# bir üst klasöre cigaratte_detect adında bir klasör oluşturur
if not os.path.exists('../results/cigaratte_detect'):
    os.makedirs('../results/cigaratte_detect')

# CIGARATTE DETECT AI MODEL
if MODEL == 'MOBILENETV2':
    model,gen = mobilenetv2_model.return_model_and_generator()
elif MODEL == 'EFFICIENTNETB3':
    model = efficientnetb3_model.load_model()
counter = 0



def predict_smoker(image_path = None, image_data = None, msgKey = None):
    global counter
    print('CHECKING CIGARATTE SMOKER...')
    
    if image_data:
        image_data.save('test.jpg')
        image_path = 'test.jpg' 

    prediction = None

    if MODEL == 'MOBILENETV2':
        prediction = mobilenetv2_model.predict_cigaratte_smoker(model, gen, image_path)
    elif MODEL == 'EFFICIENTNETB3':
        prediction = efficientnetb3_model.predict(model, image_path)
    counter += 1

    if SAVE_RESULTS:
        image_data.save('../results/cigaratte_detect/cigaratte_pred_' + str(counter) + '_' + prediction + '.jpg')
    
    # send results to kafka
    value = json.dumps({'prediction': prediction, 'croppedPersonKey': msgKey})
    print('SENDING VALUE TO KAFKA: ', value)
    producer.produce('smokerResults', key=msgKey, value=value)
    
    if SENDING_METHOD == 'flush':
        producer.flush()
    if SENDING_METHOD == 'poll':
        producer.poll(0)

    counter += 1
    

    print('--'*40)
    print('PREDICTION: ', prediction)
    print('--'*40)


try:
    if ENABLE_UPSAMPLING:
        consumer.subscribe(['upsampledPersonByte'])
        print('SUBSCRIBED TO TOPIC: upsampledPersonByte')
    else:
        consumer.subscribe(['croppedPersonByte'])
        print('SUBSCRIBED TO TOPIC: croppedPersonByte')
    print('CIGARATTE DETECT SERVER STARTED')
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
            predict_smoker(image_data=img, msgKey=msgKey)

            
finally:
    # Close down consumer to commit final offsets.
    consumer.close()


