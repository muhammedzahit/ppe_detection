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
from multiprocessing import Process

app = Flask(__name__)
app.secret_key = 'random'

@app.route('/')
def index():
    return 'Hello World'

@app.route('/image/<type>/<key>')
def image(type, key):
    # check image exists if not return 404
    if not os.path.exists('images/' + type + '/' + key + '.jpg'):
        return 'Key not found'
    
    # get image from images folder
    image = cv2.imread('images/' + type + '/' + key + '.jpg')
    
    # convert image to bytes
    _, buffer = cv2.imencode('.jpg', image)

    # return image
    return Response(buffer.tobytes(), mimetype='image/jpeg')

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, port=5010)


