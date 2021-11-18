if False:
    import unittest
    import helpers.dbg as hdbg
    import boto3
    import logging
    from moto import mock_ec2

    import infra.scripts.aws.aws_manager as isawawma

    """
    Unit tests for the aws_manager class
    Double check to not make real calls by exporting fake credentials as env variables
        
    """
    _LOG = logging.getLogger(__name__)

    # Constants used for throughout tests.
    DEFAULT_KEYNAME: str = "ec2-key-pair"
    DEFAULT_REGION: str = "us-east-1"

    @mock_ec2
    class Test_AWSManager(unittest.TestCase):
        def test_create_instance(self) -> None:
            # Already one instance in the list.
            instance_count = 1
            image_id = "ami-083654bd07b5da81d"
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
