FROM 083233266530.dkr.ecr.us-east-2.amazonaws.com/amp_env:latest

RUN  apt-get update \
  && apt-get install -y wget \
  && apt-get install -y unzip \
  && apt-get install -y xvfb \
  && apt-get install -y libxtst6 \
  && apt-get install -y libxrender1 \
  && apt-get install -y libxi6 \
	&& apt-get install -y x11vnc \
  && apt-get install -y socat \
  && apt-get install -y software-properties-common

# Setup IB TWS
RUN mkdir -p /opt/TWS
WORKDIR /opt/TWS
RUN wget -q http://cdn.quantconnect.com/interactive/ibgateway-latest-standalone-linux-x64-v974.4g.sh
RUN chmod a+x ibgateway-latest-standalone-linux-x64-v974.4g.sh

# Setup  IBController
RUN mkdir -p /opt/IBController/ && mkdir -p /opt/IBController/Logs
WORKDIR /opt/IBController/
RUN wget -q http://cdn.quantconnect.com/interactive/IBController-QuantConnect-3.2.0.5.zip
RUN unzip ./IBController-QuantConnect-3.2.0.5.zip
RUN chmod -R u+x *.sh && chmod -R u+x Scripts/*.sh

WORKDIR /

# Install TWS
RUN yes n | /opt/TWS/ibgateway-latest-standalone-linux-x64-v974.4g.sh

ENV DISPLAY :0

ADD entrypoint.sh entrypoint.sh
ADD ./vnc/xvfb_init /etc/init.d/xvfb
ADD ./vnc/vnc_init /etc/init.d/vnc
ADD ./vnc/xvfb-daemon-run /usr/bin/xvfb-daemon-run

RUN chmod -R u+x entrypoint.sh \
  && chmod -R 777 /usr/bin/xvfb-daemon-run \
  && chmod 777 /etc/init.d/xvfb \
  && chmod 777 /etc/init.d/vnc

# Below files copied during build to enable operation without volume mount
# COPY ./ib/IBController.ini /root/IBController/IBController.ini
# COPY ./ib/jts.ini /root/Jts/jts.ini

# CMD bash entrypoint.sh
WORKDIR /amp
