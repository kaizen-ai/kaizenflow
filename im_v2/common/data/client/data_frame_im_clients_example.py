"""
Import as:

import im_v2.common.data.client.data_frame_im_clients_example as imvcdcdfimce
"""


import im_v2.common.data.client.data_frame_im_clients as imvcdcdfimc

# TODO(Grisha): `im` should not depend on `market_data`.
import market_data.market_data_example as mdmadaex


def get_DataFrameImClient_example1() -> imvcdcdfimc.DataFrameImClient:
    """
    Build a `ImClient` backed by data stored in a dataframe.
    """
    # Generate input dataframe for client initialization.
    df = mdmadaex.get_im_client_market_data_df1()
    # Get universe from unique full symbols in the input dataframe.
    universe = df["full_symbol"].unique().tolist()
    # Init the client for testing.
    resample_1min = False
    im_client = imvcdcdfimc.DataFrameImClient(df, universe, resample_1min)
    return im_client
