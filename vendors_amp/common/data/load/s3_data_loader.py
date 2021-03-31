import vendors_amp.common.data.load.data_loader as vcdlda


# TODO(*): Move it to data_loader.py
class AbstractS3DataLoader(vcdlda.AbstractDataLoader):
    """
    Interface for class which reads the data from S3 for a given security.
    """
