FROM 083233266530.dkr.ecr.us-east-2.amazonaws.com/ib_connect:dev

ADD ./vendors_amp/ib/connect/vnc/xvfb_init /etc/init.d/xvfb
ADD ./vendors_amp/ib/connect/vnc/vnc_init /etc/init.d/vnc
ADD ./vendors_amp/ib/connect/vnc/xvfb-daemon-run /usr/bin/xvfb-daemon-run

ADD ./helpers /amp/helpers
ADD ./vendors_amp/ib/connect /amp/vendors_amp/ib/connect
