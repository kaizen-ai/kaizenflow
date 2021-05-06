FROM python:3.7-slim-buster

# TODO(gp): Move all this in various scripts to clean it up.
RUN apt-get update \
  && apt-get install -y wget \
  && apt-get install -y unzip \
  && apt-get install -y xvfb \
  && apt-get install -y libxtst6 \
  && apt-get install -y libxrender1 \
  && apt-get install -y libxi6 \
  && apt-get install -y x11vnc \
  && apt-get install -y socat \
  && apt-get install -y software-properties-common

COPY devops/requirements.txt /requirements.txt
# TODO(gp): Replace pip with poetry.
RUN pip install -r /requirements.txt

# Setup IB TWS.
RUN mkdir -p /opt/TWS
WORKDIR /opt/TWS
RUN wget -q http://cdn.quantconnect.com/interactive/ibgateway-latest-standalone-linux-x64-v974.4g.sh
RUN chmod a+x ibgateway-latest-standalone-linux-x64-v974.4g.sh

# Setup IBController.
RUN mkdir -p /opt/IBController/ && mkdir -p /opt/IBController/Logs
WORKDIR /opt/IBController/
RUN wget -q http://cdn.quantconnect.com/interactive/IBController-QuantConnect-3.2.0.5.zip && \
    unzip ./IBController-QuantConnect-3.2.0.5.zip && \
    rm ./IBController-QuantConnect-3.2.0.5.zip
RUN chmod -R u+x *.sh && chmod -R u+x Scripts/*.sh

WORKDIR /

# Install TWS.
RUN yes n | /opt/TWS/ibgateway-latest-standalone-linux-x64-v974.4g.sh

ENV DISPLAY :0

WORKDIR /app

# TODO(gp): Move this to devops/docker_build.
COPY vnc/xvfb_init /etc/init.d/xvfb
COPY vnc/vnc_init /etc/init.d/vnc
COPY vnc/xvfb-daemon-run /usr/bin/xvfb-daemon-run

#COPY ./helpers /app/helpers
# TODO(gp): Why moving these files?
COPY devops/docker_scripts /app/scripts
COPY devops/docker_build/entrypoints/entrypoint.sh /app/entrypoint.sh