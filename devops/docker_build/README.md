i docker_release_dev_image --no-push-to-repo; tg.py

docker run -it 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:local bash

du -hs /* | sort -h


root@c67d5c69d37e:/app# du -hs /venv/* | sort -h
0       /venv/lib64
4.0K    /venv/include
4.0K    /venv/pyvenv.cfg
40K     /venv/etc
352K    /venv/bin
9.2M    /venv/share
1.1G    /venv/lib
