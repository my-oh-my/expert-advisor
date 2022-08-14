from abc import ABC, abstractmethod

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from ea.misc.utils import sign


class Position:
    def __init__(self, market, open_date_time, open_price, stop_loss=None, take_profit=None):
        self.market = market
        self.open_date_time = open_date_time
        self.open_price = open_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit

    def get_result_for_long(self, close_date_time, price):
        p_n_l = price - self.open_price

        return dict(
            market=self.market,
            open_date_time=self.open_date_time,
            open_price=self.open_price,
            close_date_time=close_date_time,
            close_price=price,
            p_n_l=p_n_l,
            p_n_l_sing=sign(p_n_l)
        )

    def get_result_for_short(self, close_date_time, price):
        p_n_l = self.open_price - price

        return dict(
            market=self.market,
            open_date_time=self.open_date_time,
            open_price=self.open_price,
            close_date_time=close_date_time,
            close_price=price,
            p_n_l=p_n_l,
            p_n_l_sing=sign(p_n_l)
        )


class Backtest(ABC):
    def __init__(self, settings: dict):
        self._settings = settings

    @staticmethod
    def aggregate_results(scenario_name, dataframe: DataFrame) -> DataFrame:
        all_aggregation_series = dataframe['p_n_l'].agg(['count', 'sum', 'mean'])
        all_aggregation_df = pd.DataFrame(all_aggregation_series, index=None).T
        all_aggregation_df.insert(0, 'scenario', [scenario_name])

        return all_aggregation_df.set_index('scenario')

    def to_file(self, scenario_name, dataframe, output_file):
        all_aggregation_df = self.aggregate_results(scenario_name, dataframe)

        series = all_aggregation_df.iloc[0]
        if series['sum'] > 0:
            as_list = series.tolist()
            as_list.insert(0, scenario_name)

            with open(output_file, 'a+') as f:
                f.write(tabulate([as_list], tablefmt='plain') + '\n')

    def to_console(self, scenario_name, dataframe):
        all_aggregation_df = self.aggregate_results(scenario_name, dataframe)

        sub_aggregation_df = dataframe.groupby(['market', 'p_n_l_sing'])['p_n_l'].agg(
            ['count', 'sum', 'mean'])

        final_df = pd.concat([all_aggregation_df, sub_aggregation_df], axis=0)
        print(final_df)

    def run_backtest(self, scenario_name, dataframe: DataFrame, output_file: str = None):
        backtest_df = self.run(dataframe)

        if not backtest_df.empty:
            if output_file is None:
                self.to_console(scenario_name, backtest_df)
            else:
                self.to_file(scenario_name, backtest_df, output_file)

    @abstractmethod
    def run(self, dataframe: DataFrame) -> DataFrame:
        pass
