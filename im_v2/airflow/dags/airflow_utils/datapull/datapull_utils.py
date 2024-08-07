"""
Import as:

import im_v2.airflow.dags.airflow_utils.datapull.datapull_utils as imvadauddu
"""

from typing import Dict, List


def get_table_name(vendor: str, data_type: str, contract_type: str) -> str:
    """
    Get database table name for the given vendor, data type and contract type.

    This function has the same behavior as `get_im_db_table_name_from_signature`
    in `data_schema/dataset_schema_utils.py`.

    :param vendor: vendor name, e.g. "binance"
    :param data_type: data type, e.g. "trades", "bid_ask"
    :param contract_type: contract type, e.g. "spot", "future"
    """
    table_name = f"{vendor}_{data_type}_{contract_type}"
    if data_type == "bid_ask":
        table_name += "_raw"
    if data_type == "trades":
        table_name += "_downloaded_all"
    return table_name


def append_params_to_command(
    components_dict: Dict, command: List[str], params: Dict
) -> List[str]:
    """
    Append the parameters to the command.

    :param components_dict: Dictionary containing the components
        extracted from the DAG to be used in the command
    :param command: List containing the initial command to which the
        parameters are to be appended
    :param params: Dictionary containing the required and optional
        parameters for the command
    """
    for param in params["required"]:
        if param not in components_dict:
            raise ValueError(f"Missing required parameter: {param}")
        if param in (
            "start_ts",
            "end_ts",
            "start-timestamp",
            "end-timestamp",
            "start_time",
            "stop_time",
            "end_timestamp",
            "start_timestamp",
        ):
            command.append(f"--{param} '{components_dict[param]}'")
        else:
            command.append(f"--{param} {components_dict[param]}")
    for param in params["optional"]:
        if param in components_dict:
            command.append(f"--{param} {components_dict[param]}")
    return command


def get_download_websocket_data_command(components_dict: Dict) -> List[str]:
    """
    Get the command to download data from a websocket.

    :param components_dict: Dictionary containing the components
        extracted from the DAG to be used in the command
    """
    download_command = [
        f"/app/amp/im_v2/ccxt/data/extract/download_exchange_data_to_db_periodically.py",
        "--method 'websocket'",
        "--aws_profile 'ck'",
    ]
    params = {
        "required": [
            "exchange_id",
            "universe",
            "db_table",
            "data_type",
            "contract_type",
            "vendor",
            "db_stage",
            "start_time",
            "stop_time",
            "download_mode",
            "downloading_entity",
            "action_tag",
            "data_format",
        ],
        "optional": ["bid_ask_depth", "websocket_data_buffer_size"],
    }
    components_dict["db_table"] = get_table_name(
        components_dict["vendor"],
        components_dict["data_type"],
        components_dict["contract_type"],
    )
    download_command = append_params_to_command(
        components_dict, download_command, params
    )
    # Add universe part if it is present. Anamolous parameter as it has two
    # values.
    if "id" in components_dict and "chunk_size" in components_dict:
        download_command.append(
            f"--universe_part {components_dict['chunk_size']} {components_dict['id']}"
        )
    return download_command


def get_download_bulk_data_command(components_dict: Dict) -> List[str]:
    """
    Get the command to download data in bulk.

    :param components_dict: Dictionary containing the components
        extracted from the DAG to be used in the command
    """
    download_command = [
        f"/app/amp/im_v2/common/data/extract/download_bulk.py",
        "--aws_profile 'ck'",
    ]
    params = {
        "required": [
            "end_timestamp",
            "start_timestamp",
            "vendor",
            "exchange_id",
            "universe",
            "data_type",
            "contract_type",
            "s3_path",
            "download_mode",
            "downloading_entity",
            "action_tag",
            "data_format",
        ],
        "optional": ["assert_on_missing_data", "version", "download_period"],
    }
    download_command = append_params_to_command(
        components_dict, download_command, params
    )
    if "id" in components_dict and "universe_part" in components_dict:
        download_command.append(
            f"--universe_part {components_dict['universe_part']} {components_dict['id']}"
        )
    return download_command


def get_resample_websocket_data_command(components_dict: Dict) -> List[str]:
    """
    Get the command to resample data from a websocket.

    :param components_dict: Dictionary containing the components
        extracted from the DAG to be used in the command
    """
    resample_command = [
        "/app/amp/im_v2/common/data/transform/resample_rt_bid_ask_data_periodically.py",
    ]
    params = {
        "required": [
            "db_stage",
            "start_ts",
            "end_ts",
            "resample_freq",
            "dag_signature",
        ],
        "optional": [],
    }
    resample_command = append_params_to_command(
        components_dict, resample_command, params
    )
    return resample_command


def get_resample_data_command(components_dict: Dict) -> List[str]:
    """
    Get the command to resample data.

    :param components_dict: Dictionary containing the components
        extracted from the DAG to be used in the command
    """
    resample_command = [
        f"/app/amp/im_v2/common/data/transform/resample_daily_bid_ask_data.py",
    ]
    params = {
        "required": [
            "start_timestamp",
            "end_timestamp",
            "src_signature",
            "dst_signature",
            "src_s3_path",
            "dst_s3_path",
            "bid_ask_levels",
        ],
        "optional": ["assert_all_resampled", "resample_freq"],
    }
    resample_command = append_params_to_command(
        components_dict, resample_command, params
    )
    return resample_command


def get_run_single_dataset_qa_command(components_dict: Dict) -> List[str]:
    """
    Get the command to run single dataset QA notebook.

    :param components_dict: Dictionary containing the components
        extracted from the DAG to be used in the command.
    """
    invoke_command = [
        "mkdir /.dockerenv",
        "&&",
        "invoke run_single_dataset_qa_notebook",
        "--aws-profile 'ck'",
    ]
    params = {
        "required": [
            "stage",
            "dataset-signature",
            "start-timestamp",
            "end-timestamp",
            "base-dst-dir",
        ],
        "optional": ["bid-ask-depth", "bid-ask-frequency-sec"],
    }
    invoke_command = append_params_to_command(
        components_dict, invoke_command, params
    )
    return invoke_command


def get_command_from_dag_name(components_dict: Dict) -> List[str]:
    """
    Get the command to download data from a websocket.

    :param components_dict: Dictionary containing the components
        extracted from the DAG to be used in the command
    """
    if components_dict["purpose"] == "download_websocket_data":
        command = get_download_websocket_data_command(components_dict)
    elif components_dict["purpose"] == "download_bulk_data":
        command = get_download_bulk_data_command(components_dict)
    elif components_dict["purpose"] == "resample_websocket_data":
        command = get_resample_websocket_data_command(components_dict)
    elif components_dict["purpose"] == "resample_data":
        command = get_resample_data_command(components_dict)
    elif components_dict["purpose"] == "data_qa":
        command = get_run_single_dataset_qa_command(components_dict)
    else:
        raise ValueError(f"Unsupported purpose: {components_dict['purpose']}")
    return command
