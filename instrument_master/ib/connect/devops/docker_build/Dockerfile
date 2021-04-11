FROM python:3.7-slim-buster

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

COPY ./instrument_master/ib/connect/devops/requirements.txt /requirements.txt
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

COPY ./instrument_master/ib/connect/vnc/xvfb_init /etc/init.d/xvfb
COPY ./instrument_master/ib/connect/vnc/vnc_init /etc/init.d/vnc
COPY ./instrument_master/ib/connect/vnc/xvfb-daemon-run /usr/bin/xvfb-daemon-run

COPY ./helpers /app/helpers
COPY ./instrument_master/ib/connect/devops/docker_scripts /app/scripts
COPY ./instrument_master/ib/connect/devops/docker_build/entrypoints/entrypoint.sh /app/entrypoint.sh
