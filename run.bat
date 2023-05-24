cd ./age_detect_server
start /B python server.py
cd ..
cd ./cigaratte_detect_server
start /B python server.py
cd ..
cd ./fire_detect_server
start /B python server.py
cd ..
cd ./flask-server
start /B python server.py
start /B python listen_server.py
cd ..
cd ./hardhat_detect_server
start /B python server.py
cd ..
cd ./person_detect_server
start /B python server.py
cd ..
cd ./upsampling_server
start /B python server.py
cd ..
cd ./image_database_server
start /B python server.py
pause