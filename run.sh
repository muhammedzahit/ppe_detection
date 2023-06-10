pip install -r requirements.txt &&
(cd age_detect_server &&
python3 server.py &
cd cigaratte_detect_server &&
python3 server.py &
cd fire_detect_server &&
python3 server.py &
cd flask-server &&
python3 server.py &
cd flask-server &&
python3 listen_server.py &
cd hardhat_detect_server &&
python3 server.py &
cd person_detect_server &&
python3 server.py &
cd upsampling_server &&
python3 server.py &
cd image_database_server &&
python3 server.py &
cd merger_classifiers &&
python3 age_smoker_merger.py &
cd merger_classifiers &&
python3 hardhat_age_merger.py &
cd merger_classifiers &&
python3 hardhat_smoker_merger.py &
cd merger_classifiers &&
python3 final_merger.py)
