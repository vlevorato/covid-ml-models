import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal

from covid_ml.ml.feature_engineering import prepare_data


class TestFE(unittest.TestCase):
    def test_prepare_data(self):
        # given
        df = pd.DataFrame({'date': ['2020-01-01', '2020-01-02', '2020-01-04', '2020-01-06'],
                           'value': [9, 10, 12, 10]})
        # when
        df_prepared = prepare_data(df)

        # then
        df_expected = pd.DataFrame(
            {'date': ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05', '2020-01-06'],
             'value': [9., 10., 11., 12., 11., 10.]})
        df_expected['date'] = pd.to_datetime(df_expected['date'])

        assert_frame_equal(df_expected, df_prepared)
