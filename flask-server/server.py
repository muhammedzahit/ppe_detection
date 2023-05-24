import sys
sys.path.append('../')
from flask import Flask, render_template, request, Response
from utils import read_ccloud_config, DriveAPI
from confluent_kafka import Producer
from utils import read_env
from streamPage import gen_frames
import json
import os

app = Flask(__name__)
app.secret_key = 'random'

# CONNECT TO KAFKA
client_config = read_ccloud_config('../client.txt')
print('CLIENT CONFIG',client_config)
producer = Producer(client_config)

env_config = read_env('../ENV.txt')

RESULTS = {}
COUNTER = 0
IMAGE_KEY = 0
IMAGE_DATABASE_URL = env_config['IMAGE_DATABASE_URL']
SENDING_METHOD = env_config['SENDING_METHOD']
ENABLE_DRIVE_UPLOAD = env_config['ENABLE_DRIVE_UPLOAD'] == 'True'
RAW_IMAGES_FOLDER_DRIVE_ID = env_config['RAW_IMAGES_FOLDER_DRIVE_ID']

# CONNECT TO GOOGLE DRIVE
driveAPI = None
if ENABLE_DRIVE_UPLOAD:
    driveAPI = DriveAPI('../credentials.json')

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
    global IMAGE_KEY
    for i in range(0,100):
        # check request.files[key] exits
        if 'file'+str(i) in request.files:
            f = request.files['file'+str(i)]
            data = f.stream.read()

            if ENABLE_DRIVE_UPLOAD:

                # save temp image to local
                with open('temp.jpg', 'wb') as f:
                    f.write(data)

                image_key_name = 'image' + str(IMAGE_KEY) + '.jpg'

                # upload image to google drive
                file_id = driveAPI.FileUpload('temp.jpg', image_key_name, folder_id=RAW_IMAGES_FOLDER_DRIVE_ID)
                print('File ID uploaded: ', file_id)

                value_ = json.dumps({'file_id' : file_id, 'key' : image_key_name})

                producer.produce("rawImageByte", key=str(IMAGE_KEY), value=value_)
                
                if SENDING_METHOD == 'flush':
                    producer.flush()
                if SENDING_METHOD == 'poll':
                    producer.poll(0)
            
            else:
                # check rawImage folder exists on results parent folder
                if not os.path.exists('../results/rawImage'):
                    os.makedirs('../results/rawImage')
                
                # save image to rawImage folder
                with open('../results/rawImage/image'+str(IMAGE_KEY)+'.jpg', 'wb') as f:
                    f.write(data)
                
                value_ = json.dumps({'key' : 'image'+str(IMAGE_KEY)+'.jpg', 'path' : '../results/rawImage/image'+str(IMAGE_KEY)+'.jpg'})
                
                producer.produce("rawImageByte", key=str(IMAGE_KEY), value=value_)

                if SENDING_METHOD == 'flush':
                    producer.flush()
                if SENDING_METHOD == 'poll':
                    producer.poll(0)

            IMAGE_KEY += 1
        else:
            return 'Images uploaded successfully'
        
@app.route('/monitoringPage')
def monitoring_page():
    return render_template('monitoringPage.html')

# add new data to results 
@app.route('/updateResults', methods = ['POST'])
def update_results():
    global COUNTER
    global RESULTS
    # get the data from the request
   
    content_type = request.headers['Content-Type']
    if content_type == 'application/json':
        # getting data with headers type, image_path, success
        data = request.get_json()
        typeImage = data['type']
        if typeImage not in RESULTS:
            RESULTS[typeImage] = []
       
        RESULTS[typeImage].append({'key' : data['key'],'success': data['success'], 'image_link': data['image_link'], 'pred' : data['pred']})   

    return {'message': 'Info Received'}


@app.route('/monitoringPageUploader/<typeImage>')
def monitoring_page_uploader(typeImage):
    global RESULTS
    res = []
    if typeImage in RESULTS:
        res = RESULTS[typeImage]

    return render_template('monitoringPageUploader.html', results=res)

@app.route('/test')
def test():
    return render_template('test.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True, use_reloader=False)


