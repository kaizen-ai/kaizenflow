"""
Import as:

import im_v2.common.data.qa.dataset_validator as imvcdqdava
"""

import logging
from typing import List

import helpers.hdbg as hdbg
import sorrentum_sandbox.common.validate as ssacoval


class DataFrameDatasetValidator(ssacoval.DatasetValidator):
    # TODO(Juraj): remove dependency on `logger``.
    def run_all_checks(self, datasets: List, logger: logging.Logger) -> None:
        error_msgs: List[str] = []
        logger.info("Running all QA checks:")
        for qa_check in self.qa_checks:
            if qa_check.check(datasets):
                logger.info("\t" + qa_check.get_status())
            else:
                error_msgs.append("\t" + qa_check.get_status())
        if error_msgs:
            error_msg = "\n".join(error_msgs)
            hdbg.dfatal(error_msg)
