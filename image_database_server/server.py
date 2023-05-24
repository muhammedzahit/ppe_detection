import sys
import os
sys.path.append('../')
from flask import Flask,Response, request
import cv2

app = Flask(__name__)
app.secret_key = 'random'

@app.route('/<path>')
def get_image(path):
    # change ^ character to / character
    path = path.replace('^', '/')   
    
    #get image from images folder
    image = cv2.imread(path)
    
    #convert image to bytes
    _, buffer = cv2.imencode('.jpg', image)

    #return image
    return Response(buffer.tobytes(), mimetype='image/jpeg')

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, port=5010, host='0.0.0.0')