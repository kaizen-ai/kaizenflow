import helpers.hunit_test as hunitest
import im_v2.airflow.dags.airflow_utils.misc as imvadautmi


class Test_extract_components_from_filename(hunitest.TestCase):
    def test1(self) -> None:
        """
        Check that the method correctly handles input with datapull DAG type
        and a full schema.
        """
        # Prepare input data.
        file_name = "preprod.europe.download_websocket_data.periodic_1min.ohlcv.ccxt.binance.v8.py"
        dag_type = "datapull"
        expected = {
            "stage": "preprod",
            "location": "europe",
            "purpose": "download_websocket_data",
            "period": "periodic_1min",
            "data_type": "ohlcv",
            "vendor": "ccxt",
            "exchange": "binance",
            "universe": "v8",
            "region": imvadautmi.EUROPE_REGION,
            "dag_id": "preprod.europe.download_websocket_data.periodic_1min.ohlcv.ccxt.binance.v8",
        }
        self._test_extract_components_from_filename(file_name, dag_type, expected)

    def test2(self) -> None:
        """
        Check that the method correctly handles input with datapull DAG type
        and an incomplete schema.
        """
        # Prepare input data.
        file_name = (
            "preprod.europe.download_websocket_data.periodic_1min.ohlcv.py"
        )
        dag_type = "datapull"
        expected = {
            "stage": "preprod",
            "location": "europe",
            "purpose": "download_websocket_data",
            "period": "periodic_1min",
            "data_type": "ohlcv",
            "region": imvadautmi.EUROPE_REGION,
            "dag_id": "preprod.europe.download_websocket_data.periodic_1min.ohlcv",
        }
        self._test_extract_components_from_filename(file_name, dag_type, expected)

    def test3(self) -> None:
        """
        Check that the method correctly handles input with trading DAG type and
        a full schema.
        """
        # Prepare input data.
        file_name = (
            "test.tokyo.scheduled_trading_system_observer.C11a.config1.K8s.py"
        )
        dag_type = "trading"
        expected = {
            "stage": "test",
            "location": "tokyo",
            "purpose": "scheduled_trading_system_observer",
            "model": "C11a",
            "config": "config1",
            "region": imvadautmi.ASIA_REGION,
            "dag_id": "test.tokyo.scheduled_trading_system_observer.C11a.config1",
        }
        self._test_extract_components_from_filename(file_name, dag_type, expected)

    def test4(self) -> None:
        """
        Check that the method correctly handles input with trading DAG type and
        an incomplete schema.
        """
        # Prepare input data.
        file_name = "test.tokyo.scheduled_trading.C11a.py"
        dag_type = "trading"
        expected = {
            "stage": "test",
            "location": "tokyo",
            "purpose": "scheduled_trading",
            "model": "C11a",
            "region": imvadautmi.ASIA_REGION,
            "dag_id": "test.tokyo.scheduled_trading.C11a",
        }
        self._test_extract_components_from_filename(file_name, dag_type, expected)

    def test5(self) -> None:
        """
        Check that an exception is raised if file name contains an invalid
        stage.
        """
        # Prepare input data.
        file_name = (
            "invalid_stage.europe.download_websocket_data.periodic_1min.ohlcv.py"
        )
        dag_type = "datapull"
        # Test method.
        with self.assertRaises(AssertionError) as cm:
            imvadautmi.extract_components_from_filename(file_name, dag_type)
        actual = str(cm.exception)
        expected = "Invalid stage: invalid_stage"
        # Verify output.
        self.assert_equal(actual, expected)

    def test6(self) -> None:
        """
        Check that an exception is raised if file name contains an invalid
        location.
        """
        # Prepare input data.
        file_name = "preprod.invalid_location.download_websocket_data.periodic_1min.ohlcv.py"
        dag_type = "datapull"
        # Test method.
        with self.assertRaises(AssertionError) as cm:
            imvadautmi.extract_components_from_filename(file_name, dag_type)
        actual = str(cm.exception)
        expected = "Invalid location: invalid_location"
        # Verify output.
        self.assert_equal(actual, expected)

    def test7(self) -> None:
        """
        Check that an exception is raised if file name contains an invalid
        dag_type.
        """
        # Prepare input data.
        file_name = (
            "preprod.europe.download_websocket_data.periodic_1min.ohlcv.py"
        )
        dag_type = "invalid_dag_type"
        # Test method.
        with self.assertRaises(ValueError) as cm:
            imvadautmi.extract_components_from_filename(file_name, dag_type)
        actual = str(cm.exception)
        expected = "Unsupported DAG type: invalid_dag_type"
        # Verify output.
        self.assert_equal(actual, expected)

    def _test_extract_components_from_filename(
        self, filename: str, dag_type: str, expected: dict
    ) -> None:
        """
        Helper method to verify output for method
        `extract_components_from_filename`.

        :param filename: The filename to test.
        :param dag_type: The type of DAG.
        :param expected: The expected dictionary of extracted
            components.
        :return: None
        """
        # Test method.
        actual = str(
            imvadautmi.extract_components_from_filename(filename, dag_type)
        )
        expected_str = str(expected)
        # Verify output.
        self.assert_equal(actual, expected_str)
