import cv2
import csv
import io
import datetime
import os

camera = cv2.VideoCapture(0)
STREAM_PAGE_FPS = 1
STREAM_PAGE_COUNTER = 1
STREAM_PAGE_SAVE_CAPTIONS = True


def gen_frames(producer):
    # check stream_page_captures folder exists
    if not os.path.exists('./stream_page_captures'):
        os.makedirs('./stream_page_captures')

    global prev_time
    global STREAM_PAGE_COUNTER
    global STREAM_PAGE_SAVE_CAPTIONS
    prev_time = datetime.datetime.now()
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
