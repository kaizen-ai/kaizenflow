"""
Import as:

import im_v2.airflow.dags.airflow_utils.ecs.operator as imvadaueop
"""

import datetime
import time
from typing import List

import airflow
from airflow.models import Variable
from airflow.providers.amazon.aws.exceptions import EcsOperatorError
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator


class EcsRunTaskOperator_withRetry(EcsRunTaskOperator):
    """
    Adds retry capability to the parent class.

    On occassion we observe failures to start an ECS task due to errors like:
        - Capacity is unavailable at this time. Please try again
        later or in a different availability zone.
    """

    # TODO(Juraj): Add error about failure to provision ephemeral storage.
    # This is a list of errors we have previously encountered and we know
    # it is possible to recover from them with a retry after a short delay.
    _KNOWN_ERRORS = [
        "Capacity is unavailable at this time.",
    ]

    # Maximum number of attempts at starting an ECS task to perform.
    _MAX_NUM_ATTEMPTS = 2
    _RETRY_DELAY_SECS = 15

    def _is_unknown_error(self, failures: List) -> bool:
        """
        Check if the received error is known or not.

        :param failures: list of failures return from the ECS API
            response
        :return: True if this is an unknown error, False otherwise
        """
        # For the sake of some room for safety,
        #  we do not check for exact string match.
        for error in self._KNOWN_ERRORS:
            for failure in failures:
                if error in failure["reason"]:
                    return False
        return True

    # Enrich the implementation from
    # https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/operators/ecs.py#L606
    def _start_task(self) -> None:
        """
        Start ECS task.

        Implementation adapted from
        https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/operators/ecs.py#L606
        augmented by adding the retry mechanism.
        """
        run_opts = {
            "cluster": self.cluster,
            "taskDefinition": self.task_definition,
            "overrides": self.overrides,
            "startedBy": self._started_by or self.owner,
        }

        if self.capacity_provider_strategy:
            run_opts["capacityProviderStrategy"] = self.capacity_provider_strategy
        elif self.launch_type:
            run_opts["launchType"] = self.launch_type
        if self.platform_version is not None:
            run_opts["platformVersion"] = self.platform_version
        if self.group is not None:
            run_opts["group"] = self.group
        if self.placement_constraints is not None:
            run_opts["placementConstraints"] = self.placement_constraints
        if self.placement_strategy is not None:
            run_opts["placementStrategy"] = self.placement_strategy
        if self.network_configuration is not None:
            run_opts["networkConfiguration"] = self.network_configuration
        if self.tags is not None:
            run_opts["tags"] = [
                {"key": k, "value": v} for (k, v) in self.tags.items()
            ]
        if self.propagate_tags is not None:
            run_opts["propagateTags"] = self.propagate_tags

        num_attempt = 1
        while num_attempt <= self._MAX_NUM_ATTEMPTS:
            response = self.client.run_task(**run_opts)
            failures = response["failures"]
            if len(failures) > 0:
                if (
                    self._is_unknown_error(failures)
                    or num_attempt == self._MAX_NUM_ATTEMPTS
                ):
                    raise EcsOperatorError(failures, response)
                self.log.warning(
                    f"Failed to start ECS task: {failures}, retrying."
                )
                num_attempt += 1
                time.sleep(self._RETRY_DELAY_SECS)
            else:
                self.log.info("ECS Task started: %s", response)
                break

        self.arn = response["tasks"][0]["taskArn"]
        self.log.info("ECS task ID is: %s", self._get_ecs_task_id(self.arn))


# Stockholm
_EUROPE_REGION = "eu-north-1"
_EFS_MOUNT_EU = "{{ var.value.efs_mount }}"
# Tokyo
ASIA_REGION = "ap-northeast-1"
_EFS_MOUNT_TOKYO = "{{ var.value.efs_mount_tokyo }}"

# Subnets and security group is not needed for EC2 deployment but 
# we keep the configuration header unified for convenience/reusability.
_ECS_PRIVATE_SUBNETS = {
    _EUROPE_REGION: [
        Variable.get("ecs_subnet1"),
        Variable.get("ecs_subnet2"),
        Variable.get("ecs_subnet3"),
    ],
    ASIA_REGION: ["subnet-0157e9f0e3cf3ea52"],
}
_ECS_PRIVATE_SECURITY_GROUPS = {
    _EUROPE_REGION: [Variable.get("ecs_security_group")],
    ASIA_REGION: ["sg-0eb7cfa7b1fb74d3a"],
}
_ECS_PUBLIC_SUBNETS = {
    _EUROPE_REGION: [
        Variable.get("ecs_public_subnet1"),
        Variable.get("ecs_public_subnet2"),
        Variable.get("ecs_public_subnet3"),
    ],
    ASIA_REGION: ["subnet-0a776f648d64bea49"],
}
_ECS_PUBLIC_SECURITY_GROUP = {
    _EUROPE_REGION: [Variable.get("ecs_public_sg")],
    ASIA_REGION: ["sg-0eb7cfa7b1fb74d3a"],
}


def get_task_definition(
    stage: str, is_full_system_run: bool, username: str
) -> str:
    """
    Get task definition name based on given arguments.

    :param stage: stage to run in, e.g. preprod
    :param is_full_system_run:
    :return: task definition name corresponding to the provided
        arguments.
    """
    # Test stage task is always ran by a specific developer.
    if stage == "test":
        assert username != ""
    # Assert logical consistency for running in prod, preprod
    # environment.
    if stage != "test":
        assert username == ""

    ecs_task_definition = "cmamp"
    if is_full_system_run and stage != "test":
        ecs_task_definition += "-system"
    if stage in ["preprod", "test"]:
        ecs_task_definition += f"-{stage}"
    if username != "":
        ecs_task_definition += f"-{username}"
    return ecs_task_definition


def get_ecs_run_task_operator(
    dag: airflow.DAG,
    stage: str,
    task_id: str,
    task_cmd: List[str],
    task_definition: str,
    cpu_units: int,
    memory_mb: int,
    *,
    region: str = _EUROPE_REGION,
    assign_public_ip: bool = False,
    ephemeralStorageGb: int = 21,
    launch_type: str = "fargate",
) -> EcsRunTaskOperator:
    """
    Get operator to run ECS task.

    :param dag: airflow DAG to assign the operator to
    :param stage: stage to run in, e.g. preprod
    :param task_id: unique identifier of the to-be created task in the
        current DAG
    :param task_cmd: bash command used as an entrypoint for the
        underlying ECS task executed by the operator.
    :param task_definition: ECS task definition to use, e.g. cmamp-
        system-preprod
    :param cpu_units: CPU units to assign to the ECS task, e.g. 256,
        512, 1024, 2048 (where 1024 = 1 CPU)
    :param memory_mb: RAM in megabytes to assign to the task, e.g. 512,
        1024
    :param assign_public_ip: specify if the task should have a public IP
        assigned, default is False
    :param ephemeralStorageGb: Size of virtual hard drive assigned to
        the task, default is 21 (lowest possible)
    :param launch_type: execution environment to run the task in (EC2 or
        Fargate)
    :return: fully configured EcsRunTaskOperator assigned to the
        provided DAG
    """
    assert launch_type in ["ec2", "fargate"]
    assert region in [_EUROPE_REGION, ASIA_REGION]

    container_overrides = [
        {
            # Currently we use a convention to match container name
            #  with the task definition to simplify our life.
            "name": task_definition,
            "command": task_cmd,
        }
    ]

    kwargs = {}
    # TODO(Juraj): split into function
    kwargs["network_configuration"] = {
        "awsvpcConfiguration": {
            "securityGroups": _ECS_PUBLIC_SECURITY_GROUP[region]
            if assign_public_ip
            else _ECS_PRIVATE_SECURITY_GROUPS[region],
            "subnets": _ECS_PUBLIC_SUBNETS[region]
            if assign_public_ip
            else _ECS_PRIVATE_SUBNETS[region],
            "assignPublicIp": "ENABLED" if assign_public_ip else "DISABLED",
        }
    }

    task = EcsRunTaskOperator_withRetry(
        task_id=task_id,
        dag=dag,
        region=region,
        cluster=Variable.get(f"{stage}_ecs_cluster"),
        task_definition=task_definition,
        launch_type=launch_type.upper(),
        overrides={
            "containerOverrides": container_overrides,
            "cpu": str(cpu_units),
            "memory": str(memory_mb),
            # EphemeralStorage Overrides 'size' setting must be at least 21.
            "ephemeralStorage": {"sizeInGiB": ephemeralStorageGb},
        },
        awslogs_group=f"/ecs/{task_definition}",
        awslogs_stream_prefix=f"ecs/{task_definition}",
        # Must be set to custom big number because of
        # https://github.com/apache/airflow/issues/33711.
        waiter_max_attempts=1000000,
        # Represent how many last lines are returned in the container logs.
        number_logs_exception=200,
        # Assign enough time to make sure any task can finish but do not allow it to get stuck
        #  in a running loop in case of a glitch.
        execution_timeout=datetime.timedelta(hours=26),
        **kwargs,
    )
    return task
