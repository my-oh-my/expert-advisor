import numpy as np
import pandas as pd

from pandas import DataFrame

from sklearn.linear_model import LinearRegression


class LongShadow:
    def __init__(self, settings):
        self.settings = settings

    def is_candle_pattern(self, df, body_ratio):
        df['is_pattern'] = np.where(
            ((abs(df['close'] - df['open']) / (df['high'] - df['low'])) < body_ratio),
            True,
            False
        )
        return df

    def get_quantile(self, data, quantile):
        return np.quantile(data, quantile)

    def is_hight_candidate(self, df, quantile):
        hight_threshold = self.get_quantile((df['high'] - df['low']), quantile)
        df['is_hight_candidate'] = np.where(
            ((df['high'] - df['low']) > hight_threshold),
            True,
            False
        )
        return df

    def calculate_beta_coefficient(self, df):
        x = np.array(range(len(df))).reshape((-1, 1))
        y = df.close

        model = LinearRegression().fit(x, y)
        coefficients = model.coef_[0]
        return coefficients[0]

    def get_beta_coefficient(self, df):
        return pd.DataFrame([[max(df.date_time), self.calculate_beta_coefficient(df)]], columns=['date_time', 'beta'])

    def apply_beta_coefficient(self, df, lookup):
        candidate_candles_idxs = df[(df['is_pattern']) & (df['is_hight_candidate'])].index.tolist()
        initial_ranges = [df.iloc[idx - lookup + 1:idx + 1] for idx in candidate_candles_idxs]

        coefficient_dfs = [self.get_beta_coefficient(rng_df) for rng_df in initial_ranges if not rng_df.empty]
        coefficient_df = pd.concat(coefficient_dfs)

        merged_df = pd.merge(
            df, coefficient_df,
            how='left',
            on=['date_time']
        )

        merged_df.fillna({'beta': 0.0}, inplace=True)

        return merged_df

    def categorize_shadow(self, row):
        open = row['open']
        close = row['close']
        high = row['high']
        low = row['low']
        result = None
        if ((high - max([open, close])) > (min([open, close]) - low)):
            result = 'up'
        elif ((high - max([open, close])) < (min([open, close]) - low)):
            result = 'down'
        else:
            result = 'unrecognized'

        return result

    def get_shadow_side(self, df):
        df['shadow_side'] = df.apply(self.categorize_shadow, axis=1)
        return df

    # PnL
    @staticmethod
    def get_open_position_signals(df):
        df['is_signal'] = np.where(
            (df['is_pattern']) & (df['is_hight_candidate']),
            True,
            False
        )
        return df

    def get_open_position_signals(dataframe: DataFrame) -> DataFrame:
        pass

    def analyze(self, dataframe: DataFrame) -> DataFrame:
        candle_body_ratio = self.settings['candle_body_ratio']
        candle_height_quantile = self.settings['candle_height_quantile']
        regression_candles_count = self.settings['regression_candles_count']
        # checking on the pattern
        pattern_checked_df = self.is_candle_pattern(dataframe, candle_body_ratio)
        # based on the quantile analysis
        hight_analyzed_df = self.is_hight_candidate(pattern_checked_df, candle_height_quantile)
        #
        with_beta_df = self.apply_beta_coefficient(hight_analyzed_df, regression_candles_count)
        # checking on shadow side
        df = self.get_shadow_side(with_beta_df)

        return self.get_open_position_signals(df)
