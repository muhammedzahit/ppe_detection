import sys
import os
sys.path.append('../')
from flask import Flask, redirect, url_for, render_template, request, make_response, session, flash
from werkzeug.utils import secure_filename
from utils import read_ccloud_config
import numpy as np
from confluent_kafka import Producer
import csv
import io

app = Flask(__name__)
app.secret_key = 'random'

UPLOAD_FOLDER = 'uploads'

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
print('CLIENT CONFIG',client_config)
producer = Producer(client_config)

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


if __name__ == '__main__':
    app.run(debug = True)
