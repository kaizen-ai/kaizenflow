#!/usr/bin/env python
"""
Download Time Series Data from Google Trends and save it into the DB.
"""

import logging
import src.db as sisebidb
import src.download as sisebido

_LOG = logging.getLogger(__name__)

if __name__ == "__main__":

    downloader = sisebido.OhlcvRestApiDownloader()
    raw_data = downloader.download(topic="summer")
    raw_data["Topic"] = ["summer"]*len(raw_data)
    raw_data = raw_data[['Topic', 'Time', 'Frequency']]

    # Save data to DB.
    db_conn = sisebidb.get_db_connection()
    saver = sisebidb.PostgresDataFrameSaver(db_conn)
    saver.save(raw_data)
