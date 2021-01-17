import inspect
import os

import pandas as pd
import vendors2.kibot.metadata.load.kibot_metadata as kmd

FILE_DIR = os.path.dirname(inspect.getfile(inspect.currentframe()))


class MockKibotMetadata(kmd.KibotMetadata):
    @classmethod
    def read_tickbidask_contract_metadata(cls) -> pd.DataFrame:
        return pd.read_csv(f"{FILE_DIR}/tickbidask_contract_metadata.txt")

    @classmethod
    def read_continuous_contract_metadata(cls) -> pd.DataFrame:
        return pd.read_csv(f"{FILE_DIR}/continuous_contract_metadata.txt")

    @classmethod
    def read_1min_contract_metadata(cls) -> pd.DataFrame:
        return pd.read_csv(f"{FILE_DIR}/1min_contract_metadata.txt")

    @classmethod
    def read_kibot_exchange_mapping(cls) -> pd.DataFrame:
        return pd.read_csv(f"{FILE_DIR}/kibot_exchange_mapping.txt")
