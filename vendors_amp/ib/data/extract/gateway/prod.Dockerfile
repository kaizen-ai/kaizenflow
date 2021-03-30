FROM 083233266530.dkr.ecr.us-east-2.amazonaws.com/ib_extract:dev

ADD ./helpers /amp/helpers
ADD ./vendors_amp/ib/data/extract/gateway /amp/vendors_amp/ib/data/extract/gateway
