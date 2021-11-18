#!/usr/bin/env python

"""

This script provides interface to interact with AWS EC2 services

"""

import logging
import boto3
import os
import sys
from typing import Optional

_LOG = logging.getLogger(__name__)


class AWS_EC2_Manager:
    """Provides interface to interact with AWS EC2 services"""

    def __init__(self, region_name: str):
        """Initialize AWS_EC2_Manager instance

        :param region_name: geographic region to connect to
        """

        # check availiblity of credentials and
        if boto3.session.Session().get_credentials() is None:
            _LOG.error(
                "Please provide AWS credentials: AWS_ACCESS_KEY_ID "
                "and AWS_SECRET_ACCESS_KEY by exporting them as env variable our specifying in ~/.aws/credentials"
            )
            sys.exit(-1)

        self.client = boto3.client(
            "ec2",
            region_name=region_name,
        )
        self.region = region_name

    def create_key_pair(self, key_name: str, out_file_path: str):
        """Create a public/private key-pair

        :param key_name: identifier of the key-pair within AWS services
        :param out_file_path: path to file where generated private key is outputted
        """
        # TODO CHECK IF KEY WITH THIS KEYNAME HAS NOT BEEN CREATED YET
        key_pair = self.client.create_key_pair(KeyName=key_name)

        private_key = key_pair["KeyMaterial"]

        # write private key to file with 400 permissions
        with os.fdopen(
            os.open(out_file_path, os.O_WRONLY | os.O_CREAT, 0o400), "w+"
        ) as handle:
            handle.write(private_key)
            _LOG.info(
                "Key Pair: '%s' successfully created. Public Key stored within AWS, Private Key stored to '%s'",
                key_name,
                out_file_path,
            )

    def create_instance(
        self,
        ami: str,
        instance_type: str,
        key_name: str,
        name_tag: str,
        root_device_name: str,
        root_device_size: int,
        root_device_type: str,
    ) -> str:
        """Create EC2 instance

        :param ami: Amazon Machine Image - ID of a template which contains software configuration of the instance
        :param instance_type: AWS instance type
        :param key_name: name of the public/private key-pair to be used to access the instance after creation
        :param name_tag: tag with key "Name" added to the instance tags
        :param root_device_name: name of the root device specific to the provided AMI
        :param root_device_size: size of the root device (in GBs)
        :param root_device_type: type of storage to use (gp2, gp3, sc1, etc.)
        :return: ID of the newly created instance
        """

        tags = []
        if name_tag:
            tags.append(
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": name_tag,
                        },
                    ],
                }
            )

        instances = self.client.run_instances(
            ImageId=ami,
            MinCount=1,
            MaxCount=1,
            InstanceType=instance_type,
            KeyName=key_name,
            BlockDeviceMappings=[
                {
                    "DeviceName": root_device_name,
                    "Ebs": {"VolumeSize": root_device_size},
                }
            ],
            TagSpecifications=tags,
        )

        instance_id = instances["Instances"][0]["InstanceId"]
        _LOG.info("Successfully created instance with ID: %s", instance_id)
        return instance_id

    def create_volume(self, avail_zone: str, size: int) -> Optional[str]:
        """Create EBS (gp2 type) volume of specified size in specified availibility zone

        :param avail_zone: geographical availibility zone to create volume in
        :param size: volume size in GBs
        :return: Volume ID if the request was successsful, None otherwise
        """
        response = self.client.create_volume(
            AvailabilityZone=avail_zone, Size=size, VolumeType="gp2"
        )

        resp_code = response["ResponseMetadata"]["HTTPStatusCode"]

        if resp_code == 200:
            volume_id = response["VolumeId"]
            _LOG.info("EBS volume created successfully, volume ID: %s", volume_id)
            volume_id = response["VolumeId"]
            return volume_id
        else:
            _LOG.error("Request failed with error code: %i", resp_code)
            return None

    def attach_volume(
        self, instance_id: str, volume_id: str, device_name: str
    ) -> None:
        """Attach specified volume to specified instance at specified mount point

        :param instance_id: ID of the ec2 instance to attach the volume to
        :param volume_id: ID of an EBS volume to be attached
        :param device_name: name of mount point
        """
        # wait for the volume to become available in case it has just been created
        self.client.get_waiter("volume_available").wait(VolumeIds=[volume_id])

        response = self.client.attach_volume(
            Device=device_name,
            InstanceId=instance_id,
            VolumeId=volume_id,
        )

        resp_code = response["ResponseMetadata"]["HTTPStatusCode"]

        if resp_code == 200:
            _LOG.info(
                "EBS volume %s attached successfully to instance: %s",
                volume_id,
                instance_id,
            )
        else:
            _LOG.error("Request failed with error code: %i", resp_code)

    def get_instance_az(self, instance_id: str) -> str:
        """Retrieve availability zone of an ec2 instance specified by its ID

        :param instance_id: ID of the ec2 instance
        :return: availability zone of the ec2 instance

        """
        ec2 = boto3.resource("ec2", region_name=self.region)
        ec2_inst = ec2.Instance(instance_id)
        return ec2_inst.placement["AvailabilityZone"]
