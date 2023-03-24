import sys
import os
sys.path.append('../')
from flask import Flask, redirect, url_for, render_template, request, make_response, session, flash, Response
from werkzeug.utils import secure_filename
from utils import read_ccloud_config
import numpy as np
from confluent_kafka import Producer
import cv2
import csv
import io
import datetime

app = Flask(__name__)
app.secret_key = 'random'

camera = cv2.VideoCapture(0)
STREAM_PAGE_FPS = 1
STREAM_PAGE_COUNTER = 1
STREAM_PAGE_SAVE_CAPTIONS = True
prev_time = datetime.datetime.now()

UPLOAD_FOLDER = 'uploads'

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
print('CLIENT CONFIG',client_config)
producer = Producer(client_config)

def gen_frames():
    global prev_time
    global STREAM_PAGE_COUNTER
    global STREAM_PAGE_SAVE_CAPTIONS
    while True:
        success, frame = camera.read()  # read the camera frame
        if not success:
            break
        else:
            curr_time = datetime.datetime.now()
            
            ret,buffer=cv2.imencode('.jpg',frame)
            frame_buffer=buffer.tobytes()

            diff = curr_time - prev_time
            if((diff.microseconds + diff.seconds * 1000000) > (1000000/STREAM_PAGE_FPS)):
                print('saved')
                prev_time = datetime.datetime.now()
                if(STREAM_PAGE_SAVE_CAPTIONS):
                    cv2.imwrite('./stream_page_captures/a' + str(STREAM_PAGE_COUNTER) + '.jpg', frame)
                STREAM_PAGE_COUNTER += 1
                producer.produce("rawImageByte", key="key", value=frame_buffer)
                producer.flush()   

        yield(b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + frame_buffer + b'\r\n') 

@app.route('/')
def home():
    return render_template('hello.html')

@app.route('/upload_page')
def upload_page():
    return render_template('upload.html')

@app.route('/uploader', methods = ['POST'])
def uploader():
    f = request.files['file']
    data = f.stream.read()
    producer.produce("rawImageByte", key="key", value=data)
    producer.flush()
   
    return 'File Uploaded'

@app.route('/get_big_array', methods = ['POST'])
def get_array():
    data = request.json
    params = data['params']
    arr = np.array(data['arr'])
    print(params, arr.shape, str(arr))
    return "Success"

@app.route('/stream_page')
def index():
    return render_template('stream.html')

@app.route('/video_feed')
def video_feed():
    return Response(gen_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')


if __name__ == '__main__':
    app.run(debug = True)






