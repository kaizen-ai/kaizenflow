"""
Import as:

import im_v2.common.data.client.data_frame_im_clients_example as ivcdcdfice
"""


import market_data.market_data_example as mdmadaex
import im_v2.common.data.client.data_frame_im_clients as ivcdcdfic


def get_DataFrameImClient_example1(resample_1min: bool) -> ivcdcdfic.DataFrameImClient:
    """
    Build client example to test data.
    """
    # Generate dataframe for client initialization.
    df = mdmadaex.get_im_client_market_data_df1()
    # Init client for testing.
    im_client = ivcdcdfic.DataFrameImClient(df, resample_1min)
    return im_client
