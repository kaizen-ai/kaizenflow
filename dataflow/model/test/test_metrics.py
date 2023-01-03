import logging

import pandas as pd

import dataflow.model.metrics as dtfmodmetr
import helpers.hpandas as hpandas
import helpers.hunit_test as hunitest


_LOG = logging.getLogger(__name__)

def _get_result_data() -> pd.DataFrame:
    data = {
        ("vwap.ret_0.vol_adj", 101): [0.199, 0.12, 0.13, 0.3],
        ("vwap.ret_0.vol_adj", 102): [0.133, 0.2, 0.333, 0.113],
        ("vwap.ret_0.vol_adj", 103): [0, 0.23, 0.31, 0.222],
        ("vwap.ret_0.vol_adj.lead2.hat", 101): [0.22, 0.232, 0.221, 0.112],
        ("vwap.ret_0.vol_adj.lead2.hat", 102): [0.98, 0.293, 0.223, 0.32],
        ("vwap.ret_0.vol_adj.lead2.hat", 103): [0.38, 0.283, 0.821, 0.922],
    }
    start_ts = pd.Timestamp("2022-08-28 00:50:00-04:00")
    end_ts = pd.Timestamp("2022-08-28 01:05:00-04:00")
    idx = pd.date_range(start_ts, end_ts, freq="5T")
    df = pd.DataFrame(data, index=idx)
    df.columns.set_names("asset_id", level=1)
    df.index.name = "end_ts"
    _LOG.debug("result_df=\n%s", hpandas.df_to_str(df))
    return df


def _get_metrics_df() -> pd.DataFrame:
    df = _get_result_data()
    y_column_name = "vwap.ret_0.vol_adj"
    y_hat_column_name = "vwap.ret_0.vol_adj.lead2.hat"
    metrics_df = dtfmodmetr.convert_to_metrics_format(
        df, y_column_name, y_hat_column_name
    )
    _LOG.debug("metrics_df=\n%s", hpandas.df_to_str(df))
    return metrics_df


class TestConvertToMetricsFormat(hunitest.TestCase):
    def test1(self) -> None:
        metrics_df = _get_metrics_df()
        actual = hpandas.df_to_str(metrics_df)
        expected = r"""
                                            vwap.ret_0.vol_adj  vwap.ret_0.vol_adj.lead2.hat
        end_ts                    asset_id
        2022-08-28 00:50:00-04:00 101                    0.199                      0.22
                                  102                    0.133                      0.98
                                  103                    0.000                      0.38
        ...
        end_ts                    asset_id
        2022-08-28 01:05:00-04:00 101                    0.300                     0.112
                                  102                    0.113                     0.320
                                  103                    0.222                     0.922
        """
        self.assert_equal(actual, expected, fuzzy_match=True)


class TestAnnotatedMetricsDf(hunitest.TestCase):
    """
    Check that metrics df is annotated correctly given a tag_mode.
    """

    def helper(self, tag_mode: str, expected: str) -> None:
        metrics_df = _get_metrics_df()
        annotated_df = dtfmodmetr.annotate_metrics_df(metrics_df, tag_mode)
        actual = hpandas.df_to_str(annotated_df)
        self.assert_equal(actual, expected, fuzzy_match=True)

    def test1(self) -> None:
        """
        `tag_mode = "hour"`
        """
        tag_mode = "hour"
        expected = r"""
                                            vwap.ret_0.vol_adj  vwap.ret_0.vol_adj.lead2.hat  hour
        end_ts                    asset_id
        2022-08-28 00:50:00-04:00 101                    0.199                      0.22    0
                                  102                    0.133                      0.98    0
                                  103                    0.000                      0.38    0
        ...
        end_ts                    asset_id
        2022-08-28 01:05:00-04:00 101                    0.300                     0.112    1
                                  102                    0.113                     0.320    1
                                  103                    0.222                     0.922    1
        """
        self.helper(tag_mode, expected)

    def test2(self) -> None:
        """
        `tag_mode = "all"`
        """
        tag_mode = "all"
        expected = r"""
                                            vwap.ret_0.vol_adj  vwap.ret_0.vol_adj.lead2.hat   all
        end_ts                    asset_id
        2022-08-28 00:50:00-04:00 101                    0.199                      0.22  all
                                  102                    0.133                      0.98  all
                                  103                    0.000                      0.38  all
        ...
        end_ts                    asset_id
        2022-08-28 01:05:00-04:00 101                    0.300                     0.112  all
                                  102                    0.113                     0.320  all
                                  103                    0.222                     0.922  all
        """
        self.helper(tag_mode, expected)

    def test3(self) -> None:
        """
        `tag_mode = "magnitude_quantile_rank"`
        """
        tag_mode = "magnitude_quantile_rank"
        expected = r"""
                                             vwap.ret_0.vol_adj  vwap.ret_0.vol_adj.lead2.hat  magnitude_quantile_rank
        end_ts                    asset_id
        2022-08-28 00:50:00-04:00 101                    0.199                      0.22                        6
                                  102                    0.133                      0.98                        3
                                  103                    0.000                      0.38                        0
        ...
        end_ts                    asset_id
        2022-08-28 01:05:00-04:00 101                    0.300                     0.112                        9
                                  102                    0.113                     0.320                        0
                                  103                    0.222                     0.922                        3
        """
        self.helper(tag_mode, expected)
