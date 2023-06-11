### Monitoring of Occupational Health and Safety Practices in Construction Sites with Deep Learning Architectures

### Project Demo

Note: GIF files are large it may take a while to load.

![demo.webm](./Proje%20Mimarisi/demo1.gif)

![demo.webm](./Proje%20Mimarisi/demo2.gif)

### Project Members

- ##### Muhammet Zahit AYDIN

[Github](https://github.com/muhammedzahit)
[LinkedIn](https://www.linkedin.com/in/muhammed-zahid-ayd%C4%B1n-14620319a/)

- ##### Ahmet Burhan BULUT

[Github](https://github.com/burhanbulut)
[LinkedIn](https://www.linkedin.com/in/aburhanbulut/)

- ##### Murat Karakurt

[Github](https://github.com/mrtkrkrt)
[LinkedIn](https://www.linkedin.com/in/murat-karakurt-5b422119a/)

### Introduction

Due to its human-oriented nature, the construction industry is an area where occupational accidents occur more frequently than other industries. The use of personal protective equipment by workers plays an important role in preventing these accidents. Controlling the use of protective equipment with manual or sensor-based systems is costly to the employer and cannot monitor workers full-time. In recent years, CNN-based deep learning algorithms for image processing have shown great improvement. Our project aims to detect the use of personal protective equipment with deep learning models. Our deep learning models will simultaneously process the camera data inside the construction site and will go into alarm in case of any security breach.

Our ecosystem aims to simultaneously control the use of helmets and vests, fire and smoke detection, and child entry into the construction site. Due to the simultaneous operation of many cameras and deep learning models in this ecosystem, data loss creates scalability and resource allocation problems. Apache Kafka architecture was used to overcome these problems. Our study, unlike previous studies in this area, improved the resolution of camera data with SR (Super Resolution) algorithms, and model performance was increased with this approach.

Deep learning models require large amounts of data. The collection of data from within the construction site is both time-consuming for the researcher and violating the private rights of the workers. The rise of the gaming industry during the COVID-19 pandemic has allowed the development of graphics engines. At the same time, in recent years, deep learning models that make pictures from text have been able to produce photorealistic pictures. In our study, we produced synthetic data using graphics engines and deep learning models. In our study, the success of deep learning models trained with synthetic data in real life tests has been observed.

### Architecture

![Architecture](./Proje%20Mimarisi/resimler/architecture_w_2.png)

In the study, the ecosystem consists of 3 main modules. These; The Module where Deep Learning Models are Run, the Database Module where the raw image data uploaded to the ecosystem and the deep learning output results are stored on the cloud, the User Interface Module where the user can upload the image data to the ecosystem and observe the prediction results. All of these modules use the same ecosystem and need to communicate with each other seamlessly. The ecosystem uses the Apache Kafka microservice architecture to manage this communication process. Apache Kafka needs a naming system for applications communicating with each other to identify each other. Zookeeper is also used for this naming system. It ensures safe and effective transmission of messages through Kafka Broker. Brokers provide the ecosystem with the ability to tolerate error. If a Broker becomes dysfunctional for any reason, other Brokers can tolerate this error. When the dysfunctional Broker becomes active again, it updates the message flow from other Brokers. 

Thanks to Apache Kafka's scalability feature, the number of modules in the project can be increased. There is the possibility of sharing the load of new modules to distributed systems. In this way, the performance of the system can be increased. When it is desired to add a new deep learning model, by adding a new topic to Kafka, the new model is included in the system. In this way, the ecosystem is enlarged without affecting the overall performance of the system.

![Architecture](./Proje%20Mimarisi/resimler/architecture_w_1.png)

When there is a raw image data input by the video contents in the ecosystem, this image is firstly up to the Database Module. This module setup returns a unique <b>FileID</b> value representing this data. The value <b>FileID</b> has the Apache Kafka architecture <b>Raw Image</b> header.
Person Detection and Fire-Smoke Detection models from Deep Learning Models are connected to the <b>Raw Image</b> Kafka header as Consumer. Through this incoming reads through the data. The raw image witnessing of these two models processes and produces output. The Person Detection model cuts the shadow human frames within the picture, and the data it cuts ends up in the Database Module. Likewise, a unique <b>FileID</b> value is retrieved for each of these archives. Person Detection Model sends <b>FileID</b> values separately to <b>CroppedPersonImage</b> Kafka headers. The Fire-Smoke Detection model, on the other hand, makes predictions on the raw data and forwards the prediction results to the <b>Results</b> Kafka header as Producer.
The SR (Super Resolution) model is bound to the <b>CroppedPersonImage</b> Kafka header as Consumer. <b>CroppedPersonImage</b> reads the data from the Kafka header and processes this data by Image Upscaling. It loads the model outputs into the Database Module and passes the unique <b>FileID</b> values returned from this module to the <b>UpsampledPersonImage</b> Kafka header as Producer. This module can be disabled for speed control purposes.

The Hardhat-Vest, Age, and Cigarette Smoker Detection models depend on the outputs from the Person Detection model. If the SR Algorithm is turned on, these models are bound to the <b>upsampledPersonImage</b> Kafka header, otherwise <b>cropperPersonImage</b> is bound to the Kafka header as Consumer. Models predict for each human frame data and send prediction results to <b>Merger Classifier</b> Kafka headers. As a final operation, all prediction results are combined in the <b>Age-Smoker-Hardhat/Vest</b> Kafka header. Vertical scalability has been brought to the ecosystem with the architecture we have established. Vertical scalability provides increased performance and efficiency in systems with large amounts of data and high traffic, as in our study. According to the query made by the user, it is forwarded to the relevant Kafka header. 
The User Interface Module can observe the results of Deep Learning Models. The module is bound to the <b>Results</b> Kafka header as Consumer. It simultaneously pulls the results and transmits them to the User Interface. It can also provide raw data input to the UI <b>RawImage</b> Kafka header as Producer.

### General Algorithm

![Architecture](./Proje%20Mimarisi/resimler/general_algorithm.png)

### Installation

#### Requirements

- Python 3.6+
- Pip (Package Installer for Python)
- Docker
- Docker Compose
- CUDA (Optional)
- Tensorflow-GPU (Optional)
- PyTorch-GPU (Optional)

#### Linux

1. Pull required docker images

    ```bash
    docker pull bitnami/zookeeper:3.7.1-debian-11-r108
    docker pull bitnami/kafka:3.2.3-debian-11-r68
    ```

2. Run docker.sh script, if you get error try to remove zookeeper,kafka docker containers before run script.

    ```bash
    sudo bash docker.sh
    ```

3. Install required python packages

    ```bash
    pip install -r requirements.txt
    ```

4. Run run.sh script. In first run it will download required models and weights it may take a while.

    ```bash
    sudo bash run.sh
    ```

#### Windows

1. Pull required docker images

    ```bash
    docker pull bitnami/zookeeper:3.7.1-debian-11-r108
    docker pull bitnami/kafka:3.2.3-debian-11-r68
    ```
2. Run docker.bat script from file explorer. if you get error try to remove zookeeper,kafka docker containers before run script.

3. Install required python packages

    ```bash
    pip install -r requirements.txt
    ```
4. Run run.bat script from file explorer. In first run it will download required models and weights it may take a while.

