FROM 083233266530.dkr.ecr.us-east-2.amazonaws.com/ib_connect:dev

ADD ./instrument_master/ib/connect/vnc/xvfb_init /etc/init.d/xvfb
ADD ./instrument_master/ib/connect/vnc/vnc_init /etc/init.d/vnc
ADD ./instrument_master/ib/connect/vnc/xvfb-daemon-run /usr/bin/xvfb-daemon-run

ADD ./helpers /amp/helpers
ADD ./instrument_master/ib/connect /amp/instrument_master/ib/connect
