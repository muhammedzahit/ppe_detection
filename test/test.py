import sys
sys.path.append('../')
from utils import DriveAPI
from PIL import Image

driveAPI = DriveAPI('../credentials.json')

img = Image.open('test.jpg')

driveAPI.FileUpload(img, 'test.jpg')
