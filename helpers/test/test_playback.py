import logging

import pandas as pd

import helpers.unit_test as hut
import helpers.playback as plbck

_LOG = logging.getLogger(__name__)


class Test_playback1(hut.TestCase):

    def test1(self) -> None:
        """

        """
        # Create inputs.
        a = 3
        b = 2
        # Serialize through Playback.
        playback = plbck.Playback("", "", "F", a, b)
        playback.start()
        c = a + b
        output = playback.end(c)
        res = output, c
        # Freeze output.
        self.check_string(output)

    def test2(self) -> None:
        """

        """
        # Create inputs.
        a = pd.DataFrame(
         {
            'Price': [700, 250, 800, 1200]
        })
        b = pd.DataFrame(
         {
            'Price': [1, 1, 1, 1]
        })
        # Serialize through Playback.
        playback = plbck.Playback("", "", "F", a, b)
        playback.start()
        c = a + b
        output = playback.end(c)
        res = output, c
        # Try to execute code.
        #playback.test_code(output)
        # Freeze output.
        self.check_string(output)

    def test3(self) -> None:
        """

        """
        # Create inputs.
        a = "test"
        b = "case"
        # Serialize through Playback.
        playback = plbck.Playback("", "", "F", a, b)
        playback.start()
        c = a + b
        output = playback.end(c)
        res = output, c
        # Freeze output.
        self.check_string(output)

    def test4(self) -> None:
        """

        """
        # Create inputs.
        a = [1, 2, 3]
        b = [4, 5, 6]
        # Serialize through Playback.
        playback = plbck.Playback("", "", "F", a, b)
        playback.start()
        c = a + b
        output = playback.end(c)
        res = output, c
        # Freeze output.
        self.check_string(output)

    def test5(self) -> None:
        """

        """
        # Create inputs.
        a = {'1': 2}
        b = {'3': 4}
        c = {}
        # Serialize through Playback.
        playback = plbck.Playback("", "", "F_dict", a, b)
        playback.start()
        c.update(a)
        c.update(b)
        output = playback.end(c)
        res = output, c
        # Freeze output.
        self.check_string(output)
