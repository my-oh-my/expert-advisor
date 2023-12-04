from pandas import DataFrame
import numpy as np


class EngulfingLoose:
    def __init__(self, settings):
        self.settings = settings

    def with_is_signal(self, dataframe: DataFrame, candles_count: int) -> DataFrame:
        local_df = dataframe.copy()

        local_df["idx"] = local_df.index.map(int)

        local_df["recent_low"] = local_df.rolling(candles_count, min_periods=candles_count)["low"].apply(
            lambda x: x.min()
        )
        local_df["lower_bound_idx"] = local_df.rolling(candles_count, min_periods=candles_count)["low"].apply(
            lambda x: x.idxmin()
        )

        local_df["recent_high"] = local_df.rolling(candles_count, min_periods=candles_count)["high"].apply(
            lambda x: x.max()
        )
        local_df["upper_bound_idx"] = local_df.rolling(candles_count, min_periods=candles_count)["high"].apply(
            lambda x: x.idxmax()
        )

        non_nan = local_df[local_df['upper_bound_idx'].notnull()].copy()
        non_nan["lower_bound_idx"] = non_nan['lower_bound_idx'].astype(int)
        non_nan["upper_bound_idx"] = non_nan['upper_bound_idx'].astype(int)

        # is_signal = true when local min/max are of the same candle and the candle happened candles_count ago
        non_nan["is_signal"] = non_nan.apply(
            lambda x: (x.lower_bound_idx == x.upper_bound_idx) & (x.idx - x.upper_bound_idx == candles_count - 1),
            axis=1
        )

        non_nan["recent_consolidation_start"] = non_nan.apply(
            lambda x: local_df.at[x.upper_bound_idx, "date_time"],
            axis=1
        )

        current_at = non_nan.date_time.max()
        non_nan['current_at'] = current_at

        return non_nan

    def analyze(self, dataframe: DataFrame) -> DataFrame:
        return self.with_is_signal(dataframe, int(self.settings['candles_count']))

    def plot_chart(self, symbol, dataframe):
        return None

    def run_scenario(self, dataframe, symbol, plot_func=None) -> DataFrame:
        result_df = self.analyze(dataframe)

        if (not result_df.empty) and (plot_func is not None):
            plot_func(symbol, result_df)

        return result_df
