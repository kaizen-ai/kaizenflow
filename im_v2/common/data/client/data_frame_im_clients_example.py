"""
Import as:

import im_v2.common.data.client.data_frame_im_clients_example as imvcdcdfimce
"""


import core.finance as cofinanc
import im_v2.common.data.client.data_frame_im_clients as imvcdcdfimc
import im_v2.common.universe as ivcu


def get_DataFrameImClient_example1() -> imvcdcdfimc.DataFrameImClient:
    """
    Build a `ImClient` backed by data stored in a dataframe.
    """
    # Generate input dataframe and universe for client initialization.
    vendor = "example1"
    universe = ivcu.get_vendor_universe(
        vendor, version="v1", as_full_symbol=True
    )
    df = cofinanc.get_im_client_market_data_df1(universe)
    # Init the client for testing.
    resample_1min = False
    im_client = imvcdcdfimc.DataFrameImClient(df, universe, resample_1min)
    return im_client
