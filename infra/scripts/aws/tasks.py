import logging
import helpers.dbg as dbg
import sys
from invoke import task
from aws_manager import AWS_EC2_Manager

_LOG = logging.getLogger(__name__)

# Since it's not easy to add global command line options to invoke, we piggy
# back the option that already exists.
# If one uses the debug option for `invoke` we turn off the code debugging.
# TODO(gp): Check http://docs.pyinvoke.org/en/1.0/concepts/library.html#
#   modifying-core-parser-arguments
if ("-d" in sys.argv) or ("--debug" in sys.argv):
    dbg.init_logger(verbosity=logging.DEBUG)
else:
    dbg.init_logger(verbosity=logging.INFO)

## Constants used for throughout multiple tasks
DEFAULT_KEYNAME: str = "ec2-key-pair"
DEFAULT_REGION: str = "us-east-1"


@task
def aws_create_key_pair(
    ctx, key_name=DEFAULT_KEYNAME, region=DEFAULT_REGION, path_to_key="aws_ec2_key.pem"
):
    """Create a public/private key-pair which can be used to access created AWS EC2 instances

    :param ctx: state encaplusated by Invoke module itself, handed to tasks when they execute
    :param key_name: unique identifier of the key, defaults to "us-east-1"
    :param region: geographical region to which the key pair is attached to, defaults to "ec2-key-pair"
    :param path_to_key: local path to save private key to, defaults to "aws_ec2_key.pem"
    :return: None
    """
    aws_manager = AWS_EC2_Manager(region)
    aws_manager.create_key_pair(key_name, path_to_key)


@task
def aws_create_instance(
    ctx,
    ami="ami-083654bd07b5da81d",
    instance_type="t2.xlarge",
    key_name=DEFAULT_KEYNAME,
    region=DEFAULT_REGION,
    root_device_name="/dev/sda1",
    root_device_size=100,
    root_device_type="gp2",
    name_tag=None,
):
    """Create chosen EC2 instance type from specified AMI with specified root partition size

    :param ctx: state encaplusated by Invoke module itself, handed to tasks when they execute
    :param ami: amazon machine image ID, defaults to "ami-083654bd07b5da81d"
    :param instance_type: type of EC2 instance, defaults to 't2.xlarge'
    :param key_name: public/private key pair which will be used to access the instance, defaults to DEFAULT_KEYNAME
    :param region: geographical region, defaults to DEFAULT_REGION
    :param root_device_name: name of the root device of the instance, defaults to '/dev/sda1'
    :param root_device_size: size of the root device in GBs, defaults to 100
    :param root_device_type: type of storage to use (gp2, gp3, sc1, etc...)
    :param name_tag: tag with key "Name" added to the instance tags
    """
    aws_manager = AWS_EC2_Manager(region)
    instance_id = aws_manager.create_instance(
        ami,
        instance_type,
        key_name,
        name_tag,
        root_device_name,
        root_device_size,
        root_device_type,
    )


@task
def aws_attach_new_volume(
    ctx, instance_id, region=DEFAULT_REGION, size=512, device_name="/dev/sdf"
):
    """Create an EBS volume in specified region and attach it to an existing EC2 instance specified by instance ID

    :param ctx: state encaplusated by Invoke module itself, handed to tasks when they execute
    :param instance_id: ID of the EC2 to attach the volume to
    :param region: geographical region to be used
    :param size: Size of the volume in GBs, defaults to 512
    :param device_name: Mount point of the new EBS volume
    """
    aws_manager = AWS_EC2_Manager(region)
    instance_az = aws_manager.get_instance_az(instance_id)
    volume_id = aws_manager.create_volume(instance_az, size)
    aws_manager.attach_volume(instance_id, volume_id, device_name)
