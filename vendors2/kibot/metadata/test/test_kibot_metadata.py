import pytest
import unittest.mock as mock

import helpers.unit_test as hut
import vendors2.kibot.metadata.load.s3_backend as vkmls3
import vendors2.kibot.metadata.load.kibot_metadata as kmd
import vendors2.kibot.metadata.test.mocking.mock_kibot_metadata as mkmd


MAX_ROWS = 500


class TestKibotMetadata(hut.TestCase):
    def _mock_s3backend_max_rows(self):
        return mock.patch.multiple(
            kmd.KibotMetadata,
            read_kibot_exchange_mapping=vkmls3.S3Backend(MAX_ROWS).read_kibot_exchange_mapping,
            read_tickbidask_contract_metadata=vkmls3.S3Backend(
                MAX_ROWS).read_tickbidask_contract_metadata,
            read_1min_contract_metadata=vkmls3.S3Backend(MAX_ROWS).read_1min_contract_metadata,
            read_continuous_contract_metadata=vkmls3.S3Backend(
                MAX_ROWS).read_continuous_contract_metadata,
        )

    @pytest.mark.slow()
    def test_get_metadata_slow1(self) -> None:
        """Output contains all expected columns"""
        with self._mock_s3backend_max_rows():
            cls = kmd.KibotMetadata()
            exp_columns = ['Kibot_symbol', 'Description', 'StartDate',
                           'Exchange', 'Exchange_group', 'Exchange_abbreviation',
                           'Exchange_symbol', 'num_contracts', 'min_contract',
                           'max_contract', 'num_expiries', 'expiries']
            df = cls.get_metadata()
            for column in df.keys():
                self.assertIn(column, exp_columns)

    @pytest.mark.slow()
    def test_get_metadata_slow2(self) -> None:
        """Output contains an reasonable amount of rows"""
        with self._mock_s3backend_max_rows():
            cls = kmd.KibotMetadata()
            exp = 25
            act = len(cls.get_metadata().index)
            self.assertLessEqual(exp, act)

    @pytest.mark.slow()
    def test_get_metadata_slow3(self) -> None:
        """Output contains an reasonable amount of rows"""
        with self._mock_s3backend_max_rows():
            cls = kmd.KibotMetadata()
            exp = 25
            act = len(cls.get_metadata("tick-bid-ask").index)
            self.assertLessEqual(exp, act)

    @pytest.mark.slow()
    def test_get_futures_slow1(self) -> None:
        """Output contains an reasonable amount of rows"""
        with self._mock_s3backend_max_rows():
            cls = kmd.KibotMetadata()
            exp = 25
            act = len(cls.get_futures())
            self.assertLessEqual(exp, act)

    @pytest.mark.slow()
    def test_get_futures_slow2(self) -> None:
        """Output contains an reasonable amount of rows"""
        with self._mock_s3backend_max_rows():
            cls = kmd.KibotMetadata()
            exp = 25
            act = len(cls.get_futures("tick-bid-ask"))
            self.assertLess(exp, act)

    @pytest.mark.slow()
    def test_get_expiry_contract_slow1(self) -> None:
        """Output contains an reasonable amount of rows"""
        with self._mock_s3backend_max_rows():
            cls = kmd.KibotMetadata()
            exp = 25
            act = len(cls.get_expiry_contracts("ES"))
            self.assertLessEqual(exp, act)

    def test_get_zero_element1(self) -> None:
        """Valid input returns valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = [0, 1]
        exp = 0
        act = cls._get_zero_elememt(inp)
        self.assertEqual(exp, act)

    def test_get_zero_element2(self) -> None:
        """Empty input returns valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = []
        exp = None
        act = cls._get_zero_elememt(inp)
        self.assertEqual(exp, act)

    def test_get_metadata1(self) -> None:
        """Valid input returns valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = "1min"
        exp = 0
        act = len(cls.get_metadata(inp).index)
        self.assertLess(exp, act)

    def test_get_metadata2(self) -> None:
        """Valid input returns valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = "daily"
        exp = 0
        act = len(cls.get_metadata(inp).index)
        self.assertLess(exp, act)

    def test_get_metadata3(self) -> None:
        """Valid input returns valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = "tick-bid-ask"
        exp = 0
        act = len(cls.get_metadata(inp).index)
        self.assertLess(exp, act)

    def test_get_metadata4(self) -> None:
        """Incorrect input raises an error."""
        cls = mkmd.MockKibotMetadata()
        inp = "invalid-input"
        with self.assertRaises(ValueError):
            cls.get_metadata(inp)

    def test_get_metadata5(self) -> None:
        """Output contains all expected columns"""
        cls = mkmd.MockKibotMetadata()
        exp_columns = ['Kibot_symbol', 'Description', 'StartDate',
                       'Exchange', 'Exchange_group', 'Exchange_abbreviation',
                       'Exchange_symbol', 'num_contracts', 'min_contract',
                       'max_contract', 'num_expiries', 'expiries']
        df = cls.get_metadata()
        for column in df.keys():
            self.assertIn(column, exp_columns)

    def test_get_futures1(self) -> None:
        """Valid input returns valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = "1min"
        exp = 0
        act = len(cls.get_futures(inp))
        self.assertLess(exp, act)

    def test_get_futures3(self) -> None:
        """Valid input returns valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = "daily"
        exp = 0
        act = len(cls.get_futures(inp))
        self.assertLess(exp, act)

    def test_get_futures4(self) -> None:
        """Valid input returns valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = "tick-bid-ask"
        exp = 0
        act = len(cls.get_futures(inp))
        self.assertLess(exp, act)

    def test_get_futures5(self) -> None:
        """Valid input returns contextually valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = "1min"
        act = cls.get_futures(inp)
        for val in act:
            int(val)

    def test_get_futures6(self) -> None:
        """Invalid input raises an error."""
        cls = mkmd.MockKibotMetadata()
        inp = "non-existent"
        with self.assertRaises(ValueError):
            cls.get_futures(inp)

    def test_get_expiry_contracts1(self) -> None:
        """Valid input returns valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = "JY"
        exp = 0
        act = len(cls.get_expiry_contracts(inp))
        self.assertLess(exp, act)

    def test_get_expiry_contracts2(self) -> None:
        """Valid input returns contextually valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = "JY"
        act = cls.get_expiry_contracts(inp)
        for val in act:
            self.assertTrue(val.startswith("JY"), f"{val} does not start with ${inp}")

    def test_get_expiry_contracts3(self) -> None:
        """Valid input returns valid output."""
        cls = mkmd.MockKibotMetadata()
        inp = "non-existent"
        exp = 0
        act = len(cls.get_expiry_contracts(inp))
        self.assertEqual(exp, act)

    def test_kibot_hardcoded_contract_lifetime_computer1(self) -> None:
        cls = kmd.KibotHardcodedContractLifetimeComputer(260, 5)
        exp = (
            "2016-03-22",
            "2017-03-15"
        )
        act = cls.compute_lifetime("CLJ17")
        self.assertEqual(exp[0], str(act.start_date.date()))
        self.assertEqual(exp[1], str(act.end_date.date()))

    def test_kibot_hardcoded_contract_lifetime_computer2(self) -> None:
        cls = kmd.KibotHardcodedContractLifetimeComputer(260, 5)
        exp = (
            "2016-12-20",
            "2017-12-13"
        )
        act = cls.compute_lifetime("CLF18")
        self.assertEqual(exp[0], str(act.start_date.date()))
        self.assertEqual(exp[1], str(act.end_date.date()))
