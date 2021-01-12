import pandas as pd

import vendors2.kibot.metadata.load.kibot_metadata as kmd


class MockKibotMetadata(kmd.KibotMetadata):
    @classmethod
    def read_tickbidask_contract_metadata(cls) -> pd.DataFrame:
        return pd.read_csv("./tickbidask_contract_metadata.txt")

    @classmethod
    def read_continuous_contract_metadata(cls) -> pd.DataFrame:
        return pd.read_csv("./continuous_contract_metadata.txt")

    @classmethod
    def read_1min_contract_metadata(cls) -> pd.DataFrame:
        return pd.read_csv("./1min_contract_metadata.txt")

    @classmethod
    def read_kibot_exchange_mapping(cls) -> pd.DataFrame:
        return pd.read_csv("./kibot_exchange_mapping.txt")
