import sys
sys.path.append('../')
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from PIL import Image
from utils import read_ccloud_config, get_bytes_from_image_data, get_image_data_from_bytes, plot_results, read_env, DriveAPI 
from ultralytics import YOLO
import os
import numpy as np
import json

env_config = read_env('../ENV.txt')
SAVE_RESULTS = env_config['AI_MODELS_SAVE_RESULTS'] == 'True'
ENABLE_UPSAMPLING = env_config['ENABLE_UPSAMPLING'] == 'True'
SENDING_METHOD = env_config['SENDING_METHOD']

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
producer = Producer(client_config)

# BURAYI HER SERVER ICIN DEGISTIR, ONEMLI !!!!!!!!!!!!!!!!
client_config['group.id'] = 'test_server'

print('CLIENT CONFIG',client_config)
consumer = Consumer(client_config)



if __name__ == "__main__":
    obj = DriveAPI()
    i = int(input("Enter your choice: 1 - Download file, 2- Upload File, 3- Exit.\n"))
      
    if i == 1:
        f_id = input("Enter file id: ")
        f_name = input("Enter file name: ")
        obj.FileDownload(f_id, f_name)
          
    elif i == 2:
        f_path = input("Enter full file path: ")
        file_id = obj.FileUpload(f_path, name='aaaa.jpg')
        dict_ = {'file_id': file_id}
        producer.produce('rawImageByte', key='1',value=json.dumps(dict_))
        producer.flush()
        print("File ID: ", file_id)
      
    else:
        exit()
