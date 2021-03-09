import vendors_amp.common.data.load.data_loader as vcdlda


class AbstractS3DataLoader(vcdlda.AbstractDataLoader):
    """
    Interface for class which reads the data from S3 for a given security.
    """
