from typing import Dict, Any

import helpers.hgit as hgit
import helpers.hstring as hstring
import helpers.hdbg as hdbg
import os
# TODO(Juraj): At high level this module essentially performs the same thing as 
#  im_v2/common/universe/universe.py -> try to extract the common logic
#  according to DRY principle.


def _get_dataset_schema_file_path() -> str:
    """
    """
    # TODO(Juraj): Implement dynamic version resolving and remove hardcoded logic.
    ds_file_path = os.path.join(
        hgit.get_amp_abs_path(),
        "im_v2/common/data/extract/data_schema/dataset_schema_v3.json"
    )
    hdbg.dassert_path_exists(file_path)
    return ds_file_path

# TODO(Juraj): Implement dynamic version resolving.
def get_dataset_schema() -> Dict[str, Any]:
    ds_file_path = _get_dataset_schema_file_path
    return