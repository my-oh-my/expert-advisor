import numpy as np
import pandas as pd
from pandas import DataFrame


class Engulfing:
    def __init__(self, settings):
        self.settings = settings

    def condition_generator(self, dataframe: DataFrame, shift: int):
        local_df = dataframe.copy()

        condition = (local_df['low'] >= local_df.shift(shift)['low']) & (
                    local_df['high'] <= local_df.shift(shift)['high'])

        return condition

    def with_check_column(self, dataframe: DataFrame):
        local_df = dataframe.copy()
        condition = self.condition_generator(local_df, 1)

        local_df['check'] = np.where(
            # ((df['high'] - df['low']) <= allowed_candle_height) &
            condition,
            1,
            0
        )

        current_at = local_df.date_time.max()
        local_df['current_at'] = current_at

        return local_df

    # "Loops in pandas are a sin."
    def apply_counting(self, df):
        candles_count = 0
        values = []
        recent_low = None
        recent_high = None
        recent_consolidation_start = None
        recent_index = None

        for idx in range(len(df)):
            if df.loc[idx, 'check'] == 1:
                candles_count += 1
            else:
                recent_low = df.loc[idx, 'low']
                recent_high = df.loc[idx, 'high']
                recent_consolidation_start = df.loc[idx, 'date_time']
                recent_index = idx
                candles_count = 0

            values.append(
                (df.loc[idx, 'date_time'], recent_low, recent_high, recent_consolidation_start, recent_index, candles_count))

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

    def with_is_signal(self, dataframe: DataFrame) -> DataFrame:
        local_df = dataframe.copy()
        local_df['is_signal'] = local_df['candles_count'] == self.settings['candles_count']

        return local_df

    def analyze(self, dataframe: DataFrame) -> DataFrame:
        return self.with_is_signal(self.apply_counting(self.with_check_column(dataframe)))

    def plot_chart(self, symbol, dataframe):
        return None

    def run_scenario(self, dataframe, symbol, plot_func=None) -> DataFrame:
        result_df = self.analyze(dataframe)

        if (not result_df.empty) and (plot_func is not None):
            plot_func(symbol, result_df)

        return result_df
