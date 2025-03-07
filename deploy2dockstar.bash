scp solar.py piotr@docstar:~/work/solary/
scp -r simple_pid piotr@docstar:~/work/solary/
ssh piotr@docstar mkdir -p ~/work/solary/mosquitto
scp mosquitto/*.conf piotr@docstar:~/work/solary/mosquitto/
