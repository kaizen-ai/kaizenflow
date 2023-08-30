"""
Centralize checking logical dependencies between values of various arguments
for ETL scripts.
Import as:
import im_v2.common.data.qa.validate_input_args as imvcdqviar
"""
import argparse


def validate_dst_dir_arg(args: argparse.Namespace) -> None:
    """
    Validate destination directory args.
    
    :param args: destination directory args
    """
    if args.get("dst_dir"):
        valid_data_formats = ['csv','parquet']
        data_format = args.get("data_format")
        if data_format not in valid_data_formats:
            raise ValueError(f"--data_format argument cannot be {data_format} it should be one \
                    of the following formats {valid_data_formats}")
        if args.get("db_table"):
            raise ValueError(f"Invalid argument db_table present")
        if args.get("s3_path"):
            raise ValueError(f"Invalid argument s3_path present")
