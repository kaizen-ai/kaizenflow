#!/usr/bin/env python
"""
Download Time Series Data from Google Trends and save it into the DB.
"""
import argparse
import logging
import src.db as sisebidb
import src.download as sisebido
from utilities import custom_logger


def _add_download_args(
        parser: argparse.ArgumentParser,
) -> argparse.ArgumentParser:
    """
    Add the command line options for exchange download.
    """
    parser.add_argument(
        "--start_timestamp",
        required=False,
        action="store",
        type=str,
        help="Beginning of the loaded period, e.g. 2022-02-09",
    )
    parser.add_argument(
        "--end_timestamp",
        action="store",
        required=False,
        type=str,
        help="End of the loaded period, e.g. 2022-02-10",
    )
    parser.add_argument(
        "--target_table",
        action="store",
        required=True,
        type=str,
        help="Name of the db table to save data into",
    )
    parser.add_argument(
        "--use_api",
        action="store",
        required=True,
        type=str,
        help="Fetch method, swich between using a pre-build Json or using the API"
    )

    parser.add_argument(
        "--real_time_data",
        action="store",
        required=True,
        type=str,
        help="Fetch realtime data / historical"
    )
    return parser


def _parse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser = _add_download_args(parser)
    # parser = hparser.add_verbosity_arg(parser)
    return parser


# log_path = "/var/lib/app/data/"
log_path = "src/logs/"
# _LOG = custom_logger.logger(log_path + "download_to_db.py.log")


def _main(parser: argparse.ArgumentParser) -> None:
    args = parser.parse_args()
    start_timestamp = args.start_timestamp
    end_timestamp = args.end_timestamp
    target_table = args.target_table
    use_api = True if args.use_api == "True" else False
    real_time_data = True if args.real_time_data == "True" else False

    topic = "washing machines"
    topic = topic.lower()
    # _LOG.info("-------------------------------------------")
    # _LOG.info("Topic to fetch: " + topic)

    # _LOG.info("Prepping the Downloader")
    downloader = sisebido.OhlcvRestApiDownloader()

    # # _LOG.info("Downloading the data using the Google Trends API...")
    # # raw_data = downloader.download(topic.lower(), use_api=True)
    #
    # _LOG.info("Fetching the Json from /root/data/data.json")
    # raw_data = downloader.download(start_timestamp=, end_timestamp=, use_api=False)

    # if use_api:
    #     _LOG.info("Downloading the data using the Google Trends API...")
    # else:
    #     _LOG.info("Fetching the Json from /root/data/data.json")
    raw_data = downloader.download(topic=topic, start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                                   use_api=use_api, real_time_data=real_time_data)

    # _LOG.info("Json fetched and converted to DataFrame")

    # _LOG.info("Fetching a connection to postgres db...")
    db_conn = sisebidb.get_db_connection()
    print("!!connection fetched!!")
    # _LOG.info("Connection fetched")

    # _LOG.info("Creating a Saver...")
    saver = sisebidb.PostgresDataFrameSaver(db_conn)
    # _LOG.info("DB saver created")

    # _LOG.info("Saving to DB...")
    saver.save(raw_data, target_table)
    # _LOG.info("Data saved")
    print("!Done!")
    print(raw_data.head(5))


if __name__ == "__main__":
    _main(_parse())
