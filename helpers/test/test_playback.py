import logging

import pandas as pd

import helpers.unit_test as hut
import helpers.playback as plbck

_LOG = logging.getLogger(__name__)



class TestJsonRoundtrip1(hut.TestCase):
    """
    Test roundtrip conversion through jsonpickle for different types.
    """

    def test1(self) -> None:
        obj = 3
        #
        plbck.round_trip_convert(obj, logging.DEBUG)

    def test2(self) -> None:
        obj = "hello"
        #
        plbck.round_trip_convert(obj, logging.DEBUG)

    def test3(self) -> None:
        data = {
            'Product': ['Desktop Computer', 'Tablet', 'iPhone', 'Laptop'],
            'Price': [700, 250, 800, 1200]
        }
        df = pd.DataFrame(data, columns=['Product', 'Price'])
        df.index.name = "hello"
        #
        obj = df
        plbck.round_trip_convert(obj, logging.DEBUG)


class TestPlaybackInputOutput1(hut.TestCase):
    """
    Freeze the output of Playback object.
    """

    def test1(self) -> None:
        """
        Test for int inputs.
        """
        def F(a, b):
            return a + b
        # Create inputs.
        a = 3
        b = 2
        # Serialize through Playback.
        playback = plbck.Playback("assert_equal", "F", a, b)
        playback.start()
        c = F(a, b)
        code = playback.end(c)
        # Freeze output.
        self.check_string(code)
        # Execute the code.
        _LOG.debug("Testing code:\n%s", code)
        import jsonpickle
        exec(code, locals())

    # def test3(self) -> None:
    #     """
    #
    #     """
    #     # Create inputs.
    #     a = "test"
    #     b = "case"
    #     # Serialize through Playback.
    #     playback = plbck.Playback("assert_equal", "F", a, b)
    #     playback.start()
    #     c = a + b
    #     output = playback.end(c)
    #     res = output, c
    #     # Freeze output.
    #     self.check_string(output)
    #
    # def test4(self) -> None:
    #     """
    #
    #     """
    #     # Create inputs.
    #     a = [1, 2, 3]
    #     b = [4, 5, 6]
    #     # Serialize through Playback.
    #     playback = plbck.Playback("assert_equal", "F", a, b)
    #     playback.start()
    #     c = a + b
    #     output = playback.end(c)
    #     res = output, c
    #     # Freeze output.
    #     self.check_string(output)
    #
    # def test5(self) -> None:
    #     """
    #
    #     """
    #     # Create inputs.
    #     a = {'1': 2}
    #     b = {'3': 4}
    #     c = {}
    #     # Serialize through Playback.
    #     playback = plbck.Playback("assert_equal", "F_dict", a, b)
    #     playback.start()
    #     c.update(a)
    #     c.update(b)
    #     output = playback.end(c)
    #     res = output, c
    #     # Freeze output.
    #     self.check_string(output)

    def test5(self) -> None:
        """
        Test for pd.DataFrame inputs.
        """
        def F(a, b):
            return a + b
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
        playback = plbck.Playback("assert_equal", "F", a, b)
        playback.start()
        c = F(a, b)
        code = playback.end(c)
        # Freeze output.
        self.check_string(code)
        # Execute the code.
        _LOG.debug("Testing code:\n%s", code)
        import jsonpickle
        exec(code, locals())


class TestPlaybackUseCase1(hut.TestCase):

    def test1(self):
        def F(a, b):
            if use_playback:
                playback = plbck.Playback("assert_equal", "F", a, b)
                playback.start()
            c = a + b
            if use_playback:
                output = playback.end(c)
                res = output
            else:
                res = c
            return res
        # Execute without playback.
        a = 3
        b = 2
        use_playback = False
        ret = F(a, b)
        self.assertEqual(ret, 5)
        # Execute capturing the function as a unit test.
        a = 3
        b = 2
        use_playback = True
        code = F(a, b)
        self.check_string(code)
        # Execute the code.
        _LOG.debug("Testing code:\n%s", code)
        import jsonpickle
        # We need to disable the unit test generation.
        use_playback = False
        exec(code, locals())
