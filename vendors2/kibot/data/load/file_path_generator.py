import os

import vendors2.kibot.data.types as types
import vendors2.kibot.data.config as config


class FilePathGenerator:
    @staticmethod
    def generate_file_path(
        frequency: types.Frequency,
        contract_type: types.ContractType,
        symbol: str,
        ext: types.Extension,
    ) -> str:
        """Get the path to a specific kibot dataset on s3.

        Parameters as in `read_data`.
        :return: path to the file
        """

        FREQ_PATH_MAPPING = {
            types.Frequency.Daily: "daily",
            types.Frequency.Minutely: "1min",
        }

        freq_path = FREQ_PATH_MAPPING[frequency]

        CONTRACT_PATH_MAPPING = {
            types.ContractType.Continuous: "_Continuous",
            types.ContractType.Expiry: "",
        }

        contract_path = CONTRACT_PATH_MAPPING[contract_type]

        dir_name = f"All_Futures{contract_path}_Contracts_{freq_path}"
        file_path = os.path.join(dir_name, symbol)

        if ext == types.Extension.Parquet:
            # Parquet files are located in `pq/` subdirectory.
            file_path = os.path.join("pq", file_path)
            file_path += ".pq"
        elif ext == types.Extension.CSV:
            file_path += ".csv.gz"

        # TODO(amr): should we allow pointing to a local file here?
        file_path = os.path.join("s3://", config.S3_PREFIX, file_path)
        return file_path
