import sys
import os
sys.path.append('../')
from flask import Flask,Response
import cv2

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
    app.run(debug=True, use_reloader=False, port=5010, host='0.0.0.0')


