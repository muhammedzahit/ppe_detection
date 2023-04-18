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
import time
import json
from multiprocessing import Process

app = Flask(__name__)
app.secret_key = 'random'

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
print('CLIENT CONFIG',client_config)
producer = Producer(client_config)


RESULTS = {}
COUNTER = 0

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

# add new data to results 
@app.route('/updateResults/<typeImage>', methods = ['POST'])
def update_results(typeImage):
    global COUNTER
    global RESULTS
    print('RESULTS ID', id(RESULTS))
    # get the data from the request
   
    content_type = request.headers['Content-Type']
    print('CONTENT TYPE', content_type)
    if content_type == 'application/octet-stream':
        data = request.data
        print('DATA', data[0:10], typeImage)

        # check if Results folder exists in static directory
        if not os.path.exists('static/Results'):
            os.makedirs('static/Results')

        
        # save image to Results folder according to type
        with open('static/Results/' + typeImage + '_' + str(COUNTER) + '.jpg', 'wb') as f:
            f.write(data)
        
        # check RESULTS dict has key type
        if typeImage not in RESULTS:
            RESULTS[typeImage] = []
        # append path to RESULTS dict
        RESULTS[typeImage].append('static/Results/' + typeImage + '_' + str(COUNTER) + '.jpg')

        print('RESULTS', RESULTS)

        COUNTER += 1
            
            
    

    # add the data to the results
    #for data_ in data['results']:
    #    RESULTS.append(data_)
    # return the results
    return {'message': 'Image Received'}


@app.route('/monitoringPageUploader/<typeImage>')
def monitoring_page_uploader(typeImage):
    global RESULTS
    res = []
    if typeImage in RESULTS:
        res = RESULTS[typeImage]

    for i in range(0, len(res)):
        res[i] = res[i].split('/')[-1]

    return render_template('monitoringPageUploader.html', results=res)

@app.route('/test')
def test():
    return render_template('test.html')

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)


