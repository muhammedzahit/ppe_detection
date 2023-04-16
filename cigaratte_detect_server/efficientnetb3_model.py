import tensorflow as tf
import pandas as pd
import numpy as np
import patoolib
import os
import wget

def scalar(img):
    return img

def load_model():
    # check model file exists unzip model.zip 
    if not os.path.exists('./efficientnetb3_model'):
        os.makedirs('./efficientnetb3_model')

    if not os.path.exists('./efficientnetb3_model/model.h5'):
        # check rar files exists in model folder
        # if not download rar files
        if not os.path.exists('./efficientnetb3_model/model.part1.rar'):
            urls = {
                'model.part1.rar': 'https://drive.google.com/uc?export=download&id=1gfVXry2zVP2Vk3rpZ42iiqNpYFkK45m3',
                'model.part2.rar': 'https://drive.google.com/uc?export=download&id=1ahjC3ccIqHPfGqIgZDjEJqJGyZss1lZ6',
                'model.part3.rar': 'https://drive.google.com/uc?export=download&id=1RZnGlkvflOKOPQMfAUD5XgCDkAWY9vHh',
                'model.part4.rar': 'https://drive.google.com/uc?export=download&id=1p0Z55AQTqEb6unseMJVmXq63UAqE5uQD',
                'model.part5.rar': 'https://drive.google.com/uc?export=download&id=1Hmoc_KeAFcpWHmgysXumyq4crU2MF1Le',
                'model.part6.rar': 'https://drive.google.com/uc?export=download&id=1wj8C-7-Nymn8FkjrGJVYL0GHBSpL6nLO'
            }
            for file_name, url in urls.items():
                print('DOWNLOADING', file_name)
                wget.download(url, './efficientnetb3_model/'+file_name)
                print('DOWNLOADED', file_name)
        print('UNZIPPING...')
        patoolib.extract_archive('./efficientnetb3_model/model.part1.rar', outdir='./efficientnetb3_model/')

    return tf.keras.models.load_model('./efficientnetb3_model/model.h5')

def predict(model, image_path):
    # 0 : not_smoking , 1: smoking
    test_df_2 = pd.DataFrame(data = {'filepaths' : [image_path], 'labels' : 'not_smoking'})
    
    ts_gen = tf.keras.preprocessing.image.ImageDataGenerator(preprocessing_function= scalar)
    test_gen_2 = ts_gen.flow_from_dataframe( test_df_2, x_col= 'filepaths', y_col= 'labels', target_size= (224,224), class_mode= 'categorical',
                                            color_mode= 'rgb', shuffle= False, batch_size= 41)
    preds = model.predict_generator(test_gen_2)
    y_pred = np.argmax(preds, axis=1)
    print(y_pred, preds)
    labels = ['not_smoking', 'smoking']
    return labels[y_pred[0]]