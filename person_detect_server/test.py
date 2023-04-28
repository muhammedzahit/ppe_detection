import sys
sys.path.append('../')
from utils import downloadImageFromURL, DriveAPI

driveAPI = DriveAPI('../credentials.json')

driveAPI.FileDownload('1D8OMzB-KfF4kQ9YxrzND0vIrqEOEavx3', 'test.jpg')
