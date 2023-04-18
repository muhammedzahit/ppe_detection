import sys
import os
sys.path.append('../')
from flask import Flask, redirect, url_for, render_template, request, make_response, session, flash, Response
from werkzeug.utils import secure_filename
from utils import read_ccloud_config
import numpy as np
from confluent_kafka import Producer, Consumer
from utils import get_image_data_from_bytes
import cv2
import csv
import io
import datetime
from streamPage import gen_frames

app = Flask(__name__)
app.secret_key = 'random'

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
print('CLIENT CONFIG',client_config)
producer = Producer(client_config)

client_config['group.id'] = 'monitoringPage_rawImageByte'
consumer = Consumer(client_config)
consumer.subscribe(['rawImageByte'])


RESULTS = []
RAW_IMAGE_BYTE_COUNTER = 0

@app.route('/')
def index():
    return render_template('main.html')

@app.route('/streamingPage')
def stream_page():
    return render_template('stream.html')

@app.route('/video_feed')
def video_feed():
    return Response(gen_frames(producer), mimetype='multipart/x-mixed-replace; boundary=frame')

@app.route('/imageUpload')
def image_upload():
    return render_template('imageUpload.html')

@app.route('/imageUploadSuccess', methods = ['GET'])
def imageUploadSuccess():
    return render_template('main.html', message = "Your images is uploaded successfully")


@app.route('/imageUploader', methods = ['POST'])
def imageUploader():
    for i in range(0,100):
        # check request.files[key] exits
        if 'file'+str(i) in request.files:
            f = request.files['file'+str(i)]
            data = f.stream.read()
            producer.produce("rawImageByte", key="key", value=data)
            producer.flush()
        else:
            return 'Images uploaded successfully'

@app.route('/monitoringPage')
def monitoring_page():
    return render_template('monitoringPage.html')

@app.route('/monitoringPageUploader')
def monitoring_page_uploader():
    msg = consumer.poll(timeout=1.0)
    print('msg:', msg)
    if msg: 
        if msg.error():
            raise Exception('MSG ERROR')
        else:
            #msg = msg.value().decode('utf-8')
            print('IMAGE RECEIVED') 
            print(msg)
            img = get_image_data_from_bytes(msg.value())
            
            # check Results folder exists in this directory
            if not os.path.exists('Results'):
                os.makedirs('Results')

            # save image to Results folder
            img.save('Results/' + 'rawImageByte_' + str(RAW_IMAGE_BYTE_COUNTER) + '.jpg')
            RAW_IMAGE_BYTE_COUNTER += 1

    return render_template('monitoringPageUploader.html', results = RESULTS)

if __name__ == '__main__':
    app.run(debug=True)
