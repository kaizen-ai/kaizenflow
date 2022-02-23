import helpers.hunit_test as hunitest
import im.kibot.metadata.load.kibot_metadata as imkmlkime


class TestKibotHardcodedContractLifetimeComputer(hunitest.TestCase):

    def test_kibot_hardcoded_contract_lifetime_computer1(self) -> None:
        cls = imkmlkime.KibotHardcodedContractLifetimeComputer(260, 5)
        exp = ("2016-03-23", "2017-03-15")
        act = cls.compute_lifetime("CLJ17")
        self.assertEqual(exp[0], str(act.start_date.date()))
        self.assertEqual(exp[1], str(act.end_date.date()))

    def test_kibot_hardcoded_contract_lifetime_computer2(self) -> None:
        cls = imkmlkime.KibotHardcodedContractLifetimeComputer(260, 5)
        exp = ("2016-12-21", "2017-12-13")
        act = cls.compute_lifetime("CLF18")
        self.assertEqual(exp[0], str(act.start_date.date()))
        self.assertEqual(exp[1], str(act.end_date.date()))