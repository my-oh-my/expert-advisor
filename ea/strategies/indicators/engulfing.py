import numpy as np
import pandas as pd
from pandas import DataFrame


class Engulfing:
    def __init__(self, settings):
        self.settings = settings

    # @staticmethod
    # def get_open_position_signals(self, dataframe: DataFrame) -> DataFrame:
    #     candles_count = self.settings['candles_count']
    #
    #     # TODO - check if current row has count equals to candles_count?
    #     return None

    def with_check_column(self, dataframe: DataFrame):
        local_df = dataframe.copy()
        local_df['lag_high'] = local_df.shift(1)['high']
        local_df['lag_low'] = local_df.shift(1)['low']

        local_df['check'] = np.where(
            # ((df['high'] - df['low']) <= allowed_candle_height) &
            (local_df['low'] >= local_df['lag_low']) &
            (local_df['high'] <= local_df['lag_high']),
            1,
            0
        )

        current_at = local_df.date_time.max()
        local_df['current_at'] = current_at

        return local_df

    # "Loops in pandas are a sin."
    def apply_counting(self, df):
        counter = 0
        values = []
        recent_low = None
        recent_high = None
        recent_consolidation_start = None
        recent_index = None

        for idx in range(len(df)):
            if df.loc[idx, 'check'] == 1:
                counter += 1
            else:
                recent_low = df.loc[idx, 'low']
                recent_high = df.loc[idx, 'high']
                recent_consolidation_start = df.loc[idx, 'date_time']
                recent_index = idx
                counter = 0

            values.append(
                (df.loc[idx, 'date_time'], recent_low, recent_high, recent_consolidation_start, recent_index, counter))

        candles_count_df = pd.DataFrame(
            values,
            columns=['date_time', 'recent_low', 'recent_high', 'recent_consolidation_start', 'index', 'candles_count']
        )

        return pd.merge(
            df,
            candles_count_df,
            how='left',
            on=['date_time']
        )

    def analyze(self, dataframe: DataFrame) -> DataFrame:
        return self.apply_counting(self.with_check_column(dataframe))

    def plot_chart(self, symbol, dataframe):
        return None

    def run_scenario(self, dataframe, symbol, plot_func=None) -> DataFrame:
        result_df = self.analyze(dataframe)

        if (not result_df.empty) and (plot_func is not None):
            plot_func(symbol, result_df)

        return result_df
