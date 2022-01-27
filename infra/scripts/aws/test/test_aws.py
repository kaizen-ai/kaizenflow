if True:
    import logging
    import unittest

    import boto3
    from moto import mock_ec2

    import helpers as hdbg
    import sys
    sys.path.append('..')
    import aws_manager as isawawma
    """
    For import aws_manager is required import library sys and change path of importing,
    othwerwise it will not work.
    Importing like this (import infra.scripts.aws.aws_manager as isawawma)  not working
    """
    _LOG = logging.getLogger(__name__)

    # Constants used for throughout tests.
    DEFAULT_KEYNAME: str = "ec2-key-pair"
    DEFAULT_REGION: str = "us-east-1"
    @mock_ec2
    class Test_AWSManager(unittest.TestCase):
        
        def test_create_instance(self) -> None:
            # Already one instance in the list.
            client = boto3.client('ec2', DEFAULT_REGION)
            image_response = client.describe_images()
            image_id = image_response['Images'][0]['ImageId']
            # Choose image automaticly by region 
            instance_count = 1
            """
            Number of instances 
            Currect maximmum is 1
            """
            name_tag = "test"
 
            aws_manager = isawawma.AWS_EC2_Manager(DEFAULT_REGION)

            aws_manager.create_instance(
                ami=image_id,
                instance_type="t2.xlarge",
                key_name=DEFAULT_KEYNAME,
                name_tag=name_tag,
                root_device_name="/dev/sda1",
                root_device_size=100,
                root_device_type="gp2",
            )

            client = boto3.client("ec2", region_name=DEFAULT_REGION)
            instances = client.describe_instances()["Reservations"][0][
                "Instances"
            ]

            assert len(instances) == instance_count
            assert instances[0]["ImageId"] == image_id
            assert instances[0]["Tags"][0]["Value"] == name_tag
    if __name__ == "__main__":
        unittest.main()