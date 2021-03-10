#!/usr/bin/env python

import datetime
import os

import ib_insync


def get_es_data(ib):
    contract = ib_insync.Future("ES", "202103", "GLOBEX", includeExpired=True)
    print("contract=%s" % contract)
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=datetime.date(2021, 2, 1),
        durationStr="1 D",
        barSizeSetting="1 hour",
        whatToShow="TRADES",
        useRTH=True,
        formatDate=1,
    )
    print(ib_insync.util.df(bars))


ib = ib_insync.IB()
host = os.environ["IB_GW_CONNECTION_HOST"]
port = os.environ["IB_GW_CONNECTION_PORT"]
# port = 4006
print("Connecting to %s:%s" % (host, port))
ib.connect(host=host, port=port)

get_es_data(ib)

print("Disconnecting")
ib.disconnect()
print("Done")
