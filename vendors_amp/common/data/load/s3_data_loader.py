from typing import Optional

import pandas as pd

import vendors_amp.common.data.types as vkdtyp
import vendors_amp.common.data.load.data_loader as vkldla
import abc

class AbstractS3DataLoader(vkldla.AbstractDataLoader):
    """
    Interface for S3 data loader.
    """
    ...
