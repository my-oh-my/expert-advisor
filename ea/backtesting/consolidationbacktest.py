import warnings

import pandas as pd
from pandas import DataFrame
from pandas import Timestamp
from pandas.core.common import SettingWithCopyWarning

from ea.backtesting.backtesting import Backtest, Position

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)


class ConsolidationBacktest(Backtest):
    def __init__(self, settings: dict):
        super().__init__(settings)

    def collect_position(self, dataframe: DataFrame, starting_point: Timestamp) -> DataFrame:
        return dataframe[(dataframe['date_time'] > starting_point)]

    def collect_positions(self, dataframe: DataFrame) -> list[DataFrame]:
        subsets_starting_points = dataframe.loc[dataframe['open_position_at'].notnull()]['open_position_at'].tolist()
        positions = [self.collect_position(dataframe, starting_point) for starting_point in subsets_starting_points]

        return list(positions)

    def get_bullish_p_n_l(self, dataframe: DataFrame, trailing_sl: float) -> dict:
        local_df = dataframe.copy()
        local_df.reset_index(drop=True, inplace=True)

        collection = local_df.to_dict('index')

        open_price = local_df.iloc[0]['open']
        initial_sl = local_df.iloc[0]['recent_consolidation_mid']
        current_close = local_df.iloc[0]['close']

        candle_high_series = dataframe['high']
        lower_index_bound = 0

        position = Position(
            market='bullish',
            open_date_time=local_df.iloc[0]['date_time'],
            open_price=open_price
        )
        current_sl = None
        current_date = None
        sl_series = []
        for key in collection:
            current_date = collection[key]['date_time']
            is_new_sl = collection[key - 1]['is_sl_changed'] if key > 0 else False
            if is_new_sl:
                current_sl = candle_high_series[lower_index_bound:key].max() - trailing_sl
                lower_index_bound = key
            else:
                current_sl = current_sl if current_sl is not None else initial_sl

            sl_series.append(current_sl)

            current_low = collection[key]['low']
            if current_low < current_sl:
                p_n_l = position.get_result_for_long(current_date, current_sl)
                return p_n_l
            # if not already return p_n_l
            current_close = local_df.iloc[0]['close']

        p_n_l = position.get_result_for_long(current_date, current_close)

        return p_n_l

    def process_bullish_position(self, dataframe: DataFrame, trailing_sl: float) -> dict:
        local_df = dataframe.copy()

        local_df['cum_max'] = local_df['high'].cummax()
        local_df['shifted_cum_max'] = local_df['cum_max'].shift(1, fill_value=1000000.0)
        local_df['is_sl_changed'] = local_df['cum_max'] > local_df['shifted_cum_max']

        return self.get_bullish_p_n_l(local_df, trailing_sl)

    def get_bearish_p_n_l(self, dataframe: DataFrame, trailing_sl: float) -> dict:
        local_df = dataframe.copy()
        local_df.reset_index(drop=True, inplace=True)

        collection = local_df.to_dict('index')

        open_price = local_df.iloc[0]['open']
        initial_sl = local_df.iloc[0]['recent_consolidation_mid']
        current_close = local_df.iloc[0]['close']

        candle_low_series = dataframe['low']
        lower_index_bound = 0

        position = Position(
            market='bearish',
            open_date_time=local_df.iloc[0]['date_time'],
            open_price=open_price
        )
        current_sl = None
        current_date = None
        sl_series = []
        for key in collection:
            current_date = collection[key]['date_time']
            is_new_sl = collection[key - 1]['is_sl_changed'] if key > 0 else False
            if is_new_sl:
                current_sl = candle_low_series[lower_index_bound:key].min() + trailing_sl
                lower_index_bound = key
            else:
                current_sl = current_sl if current_sl is not None else initial_sl

            sl_series.append(current_sl)

            current_high = collection[key]['high']
            if current_high > current_sl:
                p_n_l = position.get_result_for_short(current_date, current_sl)
                return p_n_l
            # if not already return p_n_l
            current_close = local_df.iloc[0]['close']

        p_n_l = position.get_result_for_short(current_date, current_close)

        return p_n_l

    def process_bearish_position(self, dataframe: DataFrame, trailing_sl: float) -> dict:
        local_df = dataframe.copy()

        local_df['cum_min'] = local_df['low'].cummin()
        local_df['shifted_cum_min'] = local_df['cum_min'].shift(1, fill_value=0.0)
        local_df['is_sl_changed'] = local_df['cum_min'] < local_df['shifted_cum_min']

        return self.get_bearish_p_n_l(local_df, trailing_sl)

    def process_position(self, dataframe: DataFrame, trailing_sl: float) -> dict:
        local_df = dataframe.copy()
        position_side = dataframe.iloc[0]['position_side']

        # consider for all the subset data
        local_df['position_side'] = position_side

        p_n_l = self.process_bullish_position(dataframe, trailing_sl) \
            if position_side == 'bullish' \
            else self.process_bearish_position(dataframe, trailing_sl)

        return p_n_l

    def run(self, dataframe: DataFrame) -> DataFrame:
        positions = self.collect_positions(dataframe)
        trailing_sl = self._settings['trailing_sl']
        position_list = [self.process_position(position, trailing_sl) for position in positions if not position.empty]

        return pd.DataFrame(position_list)
