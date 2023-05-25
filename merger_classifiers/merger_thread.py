# connect local flask server and get response
import sys
sys.path.append('../')
from confluent_kafka import KafkaError, KafkaException
import json

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

def addPredToStash(parent_image_id, consumer_type, pred,stash):
    if consumer_type != 'hardhat_results':
        stash[parent_image_id]['pred_' + consumer_type] = pred

def storeMessage(producer,consumer_type, produceTopic, parent_image_id,pred, stash,result_image_id = ""):
    if parent_image_id in stash:
        # produce message to hardhatAgeMerger topic
        addPredToStash(parent_image_id, consumer_type, pred, stash)

        if result_image_id != "":
            stash[parent_image_id]['result_image_id'] = result_image_id

        value_ = stash[parent_image_id]        

        # SEND TO KAFKA
        producer.produce(topic=produceTopic, value=json.dumps(value_))
        producer.flush()
    else:
        stash[parent_image_id] = {"parent_image_id" : parent_image_id}

        if result_image_id != "":
            stash[parent_image_id]['result_image_id'] = result_image_id

        addPredToStash(parent_image_id, consumer_type, pred,stash)

def merger_thread_type(consumer, producer,consumer_type,produceTopic,stash):
    print('THREAD STARTED', consumer_type)
    while True:
        msg = consumer.poll(timeout=1.0)
        if checkMessage(msg):
            #msg = msg.value().decode('utf-8')
            msg_json = json.loads(msg.value().decode('utf-8'))
            print('MESSAGE RECEIVED : ', msg_json)

            pred=""
            if 'prediction' in msg_json:
                pred = msg_json['prediction']
            pred = str(pred)
            
            
            result_image_id = ""
            if 'result_image_id' in msg_json:
                result_image_id = msg_json['result_image_id']

            storeMessage(consumer_type=consumer_type, stash=stash,producer=producer,produceTopic=produceTopic,parent_image_id = msg_json['parent_image_id'],pred=pred, result_image_id=result_image_id)