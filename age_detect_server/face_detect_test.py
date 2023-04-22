import cv2
import numpy as np
import tensorflow as tf
from PIL import Image
from tensorflow.keras.preprocessing.image import load_img

gender_dict = {0:"Male",1:"Female"}

model = tf.keras.models.load_model('C:\\Users\\mrtkr\\Downloads\\model.h5')

face_cascade = cv2.CascadeClassifier('cascade/haarcascade_frontalface_default.xml')
img = cv2.imread('images/bebek2.jpg')
imgGray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
faces = face_cascade.detectMultiScale(        
            imgGray,
            scaleFactor=1.3,
            minNeighbors=3,
            minSize=(30, 30)
        )

count = 0
for (x,y,w,h) in faces:
    cv2.rectangle(img,(x,y),(x+w,y+h),(255,0,0),2)
    roi_color = img[y:y+h, x:x+w]
    cv2.imwrite('faces/face' + str(count) + '.jpg', roi_color)
    count += 1
    
for i in range(count):
    img = load_img("faces/face" + str(i) + ".jpg", grayscale=True)
    img = img.resize((128,128), Image.ANTIALIAS)
    img = np.array(img)
    img = img / 255
    pred = model.predict(img.reshape(1, 128, 128, 1))
    pred_gender = gender_dict[round(pred[0][0][0])] 
    pred_age = round(pred[1][0][0])
    print("Prediction: Gender = ", pred_gender," Age = ", pred_age)