- Build the local image and test it without pushing
  ```bash
  > invoke docker_release_dev_image --no-push-to-repo; tg.py
  ```

- Check the size
  ```
  > docker run -it 665840871993.dkr.ecr.us-east-1.amazonaws.com/amp:local bash

  ##> du -hs /* | sort -h

  ##> du -hs /venv/* | sort -h
  0       /venv/lib64
  4.0K    /venv/include
  4.0K    /venv/pyvenv.cfg
  40K     /venv/etc
  352K    /venv/bin
  9.2M    /venv/share
  1.1G    /venv/lib
  ```

- Release the image
  ```bash
  > invoke docker_release_dev_image --skip-tests
  ```
