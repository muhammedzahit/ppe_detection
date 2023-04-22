# Import Data Science Libraries
import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.model_selection import train_test_split

# Import visualization libraries
import matplotlib.pyplot as plt
import matplotlib.cm as cm


# Tensorflow Libraries
from tensorflow import keras
from tensorflow.keras import layers
from keras_preprocessing.image import ImageDataGenerator
from keras.layers import Dense, Dropout
from tensorflow.keras.optimizers import legacy
from tensorflow.keras import Model
# System libraries
from pathlib import Path
import os.path

import sys

def return_model_and_generator():
    test_generator = ImageDataGenerator(
        preprocessing_function=tf.keras.applications.mobilenet_v2.preprocess_input
    )

    # Load the pretained model
    pretrained_model = tf.keras.applications.MobileNetV2(
        input_shape=(224, 224, 3),
        include_top=False,
        weights='imagenet',
        pooling='max'
    )

    pretrained_model.trainable = False

    inputs = pretrained_model.input

    resize_and_rescale = tf.keras.Sequential([
    layers.experimental.preprocessing.Resizing(224,224),
    layers.experimental.preprocessing.Rescaling(1./255),
    layers.experimental.preprocessing.RandomFlip("horizontal"),
    layers.experimental.preprocessing.RandomRotation(0.1),
    layers.experimental.preprocessing.RandomZoom(0.1),
    layers.experimental.preprocessing.RandomContrast(0.1),
    ])

    x = resize_and_rescale(inputs)

    x = Dense(256, activation='relu')(pretrained_model.output)
    x = Dropout(0.45)(x)
    x = Dense(256, activation='relu')(x)
    x = Dropout(0.45)(x)


    outputs = Dense(2, activation='softmax')(x)

    model = Model(inputs=inputs, outputs=outputs)

    model.compile(
        optimizer=legacy.Adam(0.00001),
        loss='categorical_crossentropy',
        metrics=['accuracy']
    )

    model.load_weights('weightsZip/my_checkpoint')
    return model, test_generator

def predict_cigaratte_smoker(model, test_generator ,image_path):
    test_df = pd.DataFrame(data = {'Filepath' : [image_path], 'Label' : 'not_smoking'})

    test_images= test_generator.flow_from_dataframe(
        dataframe=test_df,
        x_col='Filepath',
        y_col='Label',
        target_size=(224, 224),
        color_mode='rgb',
        class_mode='categorical',
        batch_size=32,
        shuffle=False
    )
    # Predict the label of the test_images
    pred = model.predict(test_images)
    pred = np.argmax(pred,axis=1)
    preds = {1:'smoking', 0:'not_smoking'}
    return preds[pred[0]]
    