import pandas as pd
from pandas import DataFrame

from ea.misc.utils import is_nan
from ea.backtesting.backtesting import Backtest, Position


class WavesBacktest(Backtest):
    def __init__(self, settings: dict):
        super().__init__(settings)

    def run(self, dataframe: DataFrame) -> DataFrame:
        local_df = dataframe.copy()
        collection = local_df.to_dict("index")

        flag = None
        break_confirmation_price = None
        position: Position = None

        backtest_result = []

        for key in collection:
            current_date_time = collection[key]['date_time']

            current_market = collection[key]['market']
            current_open = collection[key]['open']
            current_close = collection[key]['close']
            current_high = collection[key]['high']
            current_low = collection[key]['low']
            #
            if flag == 'bullish':
                if position is None:
                    # Handle first candle after confirmation
                    if current_open > break_confirmation_price:
                        # Opening new position
                        position = Position(
                            market='bullish',
                            open_date_time=current_date_time,
                            open_price=current_open,
                            stop_loss=break_confirmation_price
                        )
                        if current_low < break_confirmation_price:
                            # S/L during first candle
                            result = position.get_result_for_long(current_date_time, break_confirmation_price)
                            backtest_result.append(result)
                            position = None
                            flag = None
                    elif current_open < break_confirmation_price:
                        # No position open - since open price out of range
                        flag = None
                        # print('Signal was not confirmed')
                elif position is not None:
                    if current_low < break_confirmation_price:
                        # S/L
                        flag = None
                        result = position.get_result_for_long(current_date_time, break_confirmation_price)
                        backtest_result.append(result)
                        position = None
            elif flag == 'bearish':
                if position is None:
                    # Handle first candle after confirmation
                    if current_open < break_confirmation_price:
                        # Opening new position
                        position = Position(
                            market='bearish',
                            open_date_time=current_date_time,
                            open_price=current_open,
                            stop_loss=break_confirmation_price
                        )
                        if current_high > break_confirmation_price:
                            # S/L during first candle
                            flag = None
                            result = position.get_result_for_short(current_date_time, break_confirmation_price)
                            backtest_result.append(result)
                            position = None
                    elif current_open > break_confirmation_price:
                        # No position open - since open price out of range
                        flag = None
                        # print('Signal was not confirmed')
                elif position is not None:
                    if current_high > break_confirmation_price:
                        # S/L
                        flag = None
                        result = position.get_result_for_short(current_date_time, break_confirmation_price)
                        backtest_result.append(result)
                        position = None

            #
            if not is_nan(current_market):
                if current_market == 'bullish':
                    if (flag == 'bearish') & (position is not None):
                        result = position.get_result_for_short(current_date_time, current_close)
                        backtest_result.append(result)
                        position = None

                    break_confirmation_price = current_low
                    flag = 'bullish'
                elif (current_market == 'bearish') & (position is not None):
                    if flag == 'bullish':
                        result = position.get_result_for_long(current_date_time, current_close)
                        backtest_result.append(result)
                        position = None

                    break_confirmation_price = collection[key]['high']
                    flag = 'bearish'

        #
        return pd.DataFrame(backtest_result)