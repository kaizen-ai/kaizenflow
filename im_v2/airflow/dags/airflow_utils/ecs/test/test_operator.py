import datetime
import unittest.mock as umock
import pytest

import moto

import helpers.hunit_test as hunitest

# import helpers.hsystem as hsystem

# TODO(Juraj): For whatever reason the python way doesn't work, we need to manually run
# docker> sudo /venv/bin/pip install apache-airflow[amazon]==2.7.0 --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-2.7.0/constraints-3.9.txt'
# if True:
#    hsystem.system(
#        " ".join([
#            "/venv/bin/pip",
#            "install",
#            "apache-airflow[amazon]==2.7.0"
#            "--constraint",
#            "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.0/constraints-3.9.txt"
#        ]),
#    )


@pytest.mark.skip("CmTask8281 Run only manually because of missing dependency")
@moto.mock_ecs
class TestEcsRunTaskOperatorWithRetry(hunitest.TestCase):
    def setUp(self) -> None:
        self.variable_patcher = umock.patch("airflow.models.Variable")
        self.mock_variable = self.variable_patcher.start()

        # TODO(Juraj): CmTask8281 hacky import because of airflow.Variable usage at the top level.
        from airflow.models import DAG
        import im_v2.airflow.dags.airflow_utils.ecs.operator as imvadaueop

        self._MAX_NUM_ATTEMPTS = (
            imvadaueop.EcsRunTaskOperator_withRetry._MAX_NUM_ATTEMPTS
        )

        self.operator = imvadaueop.get_ecs_run_task_operator(
            DAG(
                dag_id="test_dag",
                default_args={"owner": "airflow"},
                start_date=datetime.datetime(2023, 1, 1),
            ),
            "test",
            "test_task",
            task_cmd=["echo", "hello world"],
            task_definition="cmamp-fake-task-def",
            cpu_units=256,
            memory_mb=512,
        )
        # Needed because it"s defined elsewhere, but we
        # only test standalone method _start_task.
        self.operator._started_by = None
        self.client = self.operator.client
        self.sleep_patcher = umock.patch("time.sleep")
        self.mock_sleep = self.sleep_patcher.start()

    def tearDown(self) -> None:
        self.sleep_patcher.stop()
        self.variable_patcher.stop()

    def test_start_task_with_failures(self) -> None:
        """
        Test case when len(failures) > 0 equates to True and the maximum number
        of attempts is reached.

        Verifies that EcsOperatorError is raised and run_task is called
        the maximum number of times.
        """
        # TODO(Juraj): CmTask8281 hacky import because of airflow.Variable usage at the top level.
        from airflow.providers.amazon.aws.exceptions import EcsOperatorError
        # Mock the run_task response with failures.
        with umock.patch.object(
            self.client,
            "run_task",
            return_value={
                "failures": [
                    {
                        "reason": "Capacity is unavailable at this time. Please try again later or in a different availability zone"
                    }
                ],
                "tasks": [],
            },
        ):
            with self.assertRaises(EcsOperatorError):
                self.operator._start_task()

            # Verify that run_task was called maximum number of attempts.
            self.assertEqual(
                self.client.run_task.call_count, self._MAX_NUM_ATTEMPTS
            )

    def test_start_task_success_after_retries(self) -> None:
        """
        Test case when len(failures) > 0 equates to True for the first attempt
        but the task succeeds on the second attempt.
        """
        # Mock the run_task response with failures for the first attempt
        # and success for the second attempt.
        with umock.patch.object(
            self.client,
            "run_task",
            side_effect=[
                {
                    "failures": [
                        {
                            "reason": "Capacity is unavailable at this time. Please try again later or in a different availability zone"
                        }
                    ]
                },
                {"failures": [], "tasks": [{"taskArn": "test_task_arn"}]},
            ],
        ):
            self.operator._start_task()

            self.assertEqual(self.client.run_task.call_count, 2)
            # Verify that the task ARN is set correctly.
            self.assertEqual(self.operator.arn, "test_task_arn")

    def test_start_task_success_without_retries(self) -> None:
        """
        Test case when len(failures) > 0 equates to False and the task succeeds
        without retries.
        """
        # Mock the run_task response with success.
        with umock.patch.object(
            self.client,
            "run_task",
            return_value={
                "tasks": [{"taskArn": "test_task_arn"}],
                "failures": [],
            },
        ):
            self.operator._start_task()

            self.assertEqual(self.client.run_task.call_count, 1)
            self.mock_sleep.assert_not_called()
            self.assertEqual(self.operator.arn, "test_task_arn")
