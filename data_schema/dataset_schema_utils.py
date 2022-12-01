"""
Import as:

import data_schema.dataset_schema_utils as dsdascut
"""

import logging
import os
import re
from typing import Any, Dict, Optional

import helpers.hdbg as hdbg
import helpers.hgit as hgit
import helpers.hio as hio
import helpers.hstring as hstring

# TODO(Juraj): At high level this module essentially performs the same thing as
#  im_v2/common/universe/universe.py -> try to extract the common logic
#  according to DRY principle.
_LOG = logging.getLogger(__name__)


def _get_dataset_schema_file_path(*, version: Optional[str] = None) -> str:
    """ 
    Get dataset schema file path based on version.
    
    :param version: dataset schema version (e.g. "v01"). If None it uses
      the latest version available
    :return: file path to the dataset schema file corresponding to the specified version
    """
    # TODO(Juraj): Implement dynamic version resolving and remove hardcoded logic.
    ds_file_path = os.path.join(
        hgit.get_amp_abs_path(),
        "im_v2/common/data/extract/data_schema/dataset_schema_v3.json",
    )
    hdbg.dassert_path_exists(ds_file_path)
    _LOG.info(f"Fetched dataset schema file: {ds_file_path}")
    return ds_file_path


def get_dataset_schema(*, version: Optional[str] = None) -> Dict[str, Any]:
    """
    Get dataset schema for a specified version, if version is None
     fetch the latest version of the schema.
     
    :param version: dataset schema version (e.g. "v01") to load. If None,
        the latest version is loaded.
    :return: dataset schema as a nested dictionary, e.g.
        {
            "dataset_signature":
                "download_mode.downloading_entity.action_tag",
            "token_separator_character": ".",
            "allowed_values": {
                "download_mode": ["bulk", "periodic_daily"],
                "downloading_entity": ["airflow", "manual"],
                "action_tag": ["downloaded_1sec", "resampled_1min"]
            }
            "version": "v3"
        }
    """
    # TODO(Juraj): Implement loading custom version of schema.
    if version is not None:
        raise ValueError("Dynamic custom version not supported.")
    # Load dataset schema as json.
    ds_file_path = _get_dataset_schema_file_path()
    dataset_schema = hio.from_json(ds_file_path)
    # Resolve version name.
    ds_version = hstring.extract_version_from_file_name(ds_file_path)
    # Transform version to string and remove trailing zero.
    ds_version = "v" + ".".join(map(str, ds_version)).rstrip(".0")
    # Append version from the file name to the schema dictionary.
    _LOG.info(f"Fetched dataset schema version {ds_version}")
    dataset_schema["version"] = ds_version
    # TODO(Juraj): assert that the schema file itself is well-formed.
    return dataset_schema
    
def _validate_dataset_signature_syntax(signature: str, dataset_schema: Dict[str, Any]) -> bool:
    """
    Validate syntax of a dataset signature based on 
    provided schema.
    
    For example refer to docstirng of
    data_schema/validate_dataset_signature.py
    
    :param signature: dataset signature to validate
    :param dataset_schema: dataset schema to validate against
    :return: True if the signature is syntactically well-formed, False otherwise
    """
    token_separator_char = dataset_schema["token_separator_character"]
    signature_list = signature.split(token_separator_char)
    schema_signature_list = dataset_schema["dataset_signature"].split(token_separator_char)
    is_syntax_correct = len(signature_list) == len(schema_signature_list)
    if not is_syntax_correct:
        _LOG.warning(f"Signature is malformed. Expected number of tokens \
                        is: {len(signature_list)}, actual number of tokens is: {len(schema_signature_list)}")
    return is_syntax_correct
    

def _validate_dataset_signature_semantics(signature: str, dataset_schema: Dict[str, Any]) -> bool:
    """
    Validate semantics of a dataset signature based on 
    provided schema.
    
    For example refer to docstirng of
    data_schema/validate_dataset_signature.py
    
    :param signature: dataset signature to validate
    :param dataset_schema: dataset schema to validate against
    :return: True if the signature is semantically correct, False otherwise
    """
    return False

def validate_dataset_signature(signature: str, dataset_schema: Dict[str, Any]) -> bool:
    """
    Validate syntax and semantics of a dataset signature based on 
    provided schema.
    
    For example refer to docstirng of
    data_schema/validate_dataset_signature.py
    
    :param signature: dataset signature to validate
    :param dataset_schema: dataset schema to validate against
    :return: True if the signature is syntactically AND semantically correct, False otherwise
    """
    # TODO(Juraj): Ideally this function should
    # encapsulate a final state machine-like validator
    # but for now the primitive check is good enough.
    # Check signature syntax
    is_correct_signature = _validate_dataset_signature_syntax(signature)
    # If syntax is correct, check the semantics.
    if is_correct_signature:
        is_correct_signature = _validate_dataset_signature_semantics(signatue)
    else:
        _LOG.warning("Syntax validation failed, skipping semantic validation")
    return is_correct_signature
