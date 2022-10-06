from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from pandas import DataFrame
from plotly.graph_objs import Figure
from plotly.subplots import make_subplots


class Consolidation:
    def __init__(self, settings):
        self.settings = settings

    @staticmethod
    def get_multiplication_factor(allowed_percentage_change: float, iterator: int):
        return 1 + allowed_percentage_change / (100 * iterator)

    @staticmethod
    def get_quantile(series, quantile):
        return np.quantile(series, quantile)

    def get_consolidations_time_ranges(self,
                                       dataframe: DataFrame,
                                       allowed_percent_change: float,
                                       waves_height_quantile: float) -> DataFrame:
        collection = dataframe.to_dict('index')

        wave_height_threshold = self.get_quantile(dataframe['height'], waves_height_quantile)

        consolidation_info = []

        lower_bound = 0.0
        upper_bound = 1000000.0

        iterator = 1
        bullish_multiplier = self.get_multiplication_factor(-1 * allowed_percent_change, iterator)
        bearish_multiplier = self.get_multiplication_factor(allowed_percent_change, iterator)
        for key in collection:
            break_at = collection[key]['break_time']
            current_market = collection[key]['market']
            current_break_value = collection[key]['break_value']
            current_wave_height = collection[key]['height']

            if current_market == 'bullish':
                # check
                if current_wave_height > wave_height_threshold:
                    if current_break_value < lower_bound:
                        iterator = 1
                        upper_bound = 1000000.0
                        lower_bound = current_break_value * bullish_multiplier
                        consolidation_info.append(
                            {**collection[key],
                             **{'iterator': iterator, 'lower_bound': lower_bound, 'lower_break': current_break_value}}
                        )
                    elif current_break_value > lower_bound:
                        lower_bound = current_break_value * bullish_multiplier
                        upper_bound = 1000000.0
                        consolidation_info.append(
                            {**collection[key],
                             **{'iterator': iterator, 'lower_bound': lower_bound, 'lower_break': current_break_value}}
                        )
                        iterator = 1
                elif current_wave_height < wave_height_threshold:
                    if current_break_value < lower_bound:
                        iterator = 1
                        upper_bound = 1000000.0
                        lower_bound = current_break_value * bullish_multiplier
                        consolidation_info.append(
                            {**collection[key],
                             **{'iterator': iterator, 'lower_bound': lower_bound, 'lower_break': current_break_value}}
                        )
                        # iterator = iterator + 1
                    elif current_break_value > lower_bound:
                        base_value = None
                        if iterator <= 2:
                            base_value = current_break_value
                        else:
                            base_value = lower_bound

                        lower_bound = base_value * self.get_multiplication_factor(-1 * allowed_percent_change, iterator)
                        consolidation_info.append(
                            {**collection[key],
                             **{'iterator': iterator, 'lower_bound': lower_bound, 'lower_break': current_break_value}}
                        )
                        iterator = iterator + 1
            if current_market == 'bearish':
                # check
                if current_wave_height > wave_height_threshold:
                    if current_break_value > upper_bound:
                        iterator = 1
                        lower_bound = 0
                        upper_bound = current_break_value * bearish_multiplier
                        consolidation_info.append(
                            {**collection[key],
                             **{'iterator': iterator, 'upper_bound': upper_bound, 'upper_break': current_break_value}}
                        )
                        # iterator = iterator + 1
                    elif current_break_value < upper_bound:
                        lower_bound = 0
                        upper_bound = current_break_value * bearish_multiplier
                        consolidation_info.append(
                            {**collection[key],
                             **{'iterator': iterator, 'upper_bound': upper_bound, 'upper_break': current_break_value}}
                        )
                        iterator = 1
                elif current_wave_height < wave_height_threshold:
                    if current_break_value > upper_bound:
                        iterator = 1
                        lower_bound = 0
                        upper_bound = current_break_value * bearish_multiplier
                        consolidation_info.append(
                            {**collection[key],
                             **{'iterator': iterator, 'upper_bound': upper_bound, 'upper_break': current_break_value}}
                        )
                        # iterator = iterator + 1
                    elif current_break_value < upper_bound:
                        base_value = None
                        if iterator <= 2:
                            base_value = current_break_value
                        else:
                            base_value = upper_bound

                        upper_bound = base_value * self.get_multiplication_factor(allowed_percent_change, iterator)
                        consolidation_info.append(
                            {**collection[key],
                             **{'iterator': iterator, 'upper_bound': upper_bound, 'upper_break': current_break_value}}
                        )
                        iterator = iterator + 1

        return pd.DataFrame(consolidation_info)

    def get_price_ranges(self, dataframe: DataFrame) -> DataFrame:
        consolidation_id = dataframe.break_time.max()

        dataframe['consolidation_min'] = dataframe['lower_break'].min()
        dataframe['consolidation_max'] = dataframe['upper_break'].max()
        dataframe['consolidation_id'] = consolidation_id

        return dataframe[['break_time', 'iterator', 'consolidation_min', 'consolidation_max', 'consolidation_id']]

    def get_consolidations_price_ranges(self, dataframe: DataFrame, minimum_waves_count: int) -> DataFrame:
        local_df = dataframe.copy()
        columns = ['break_time', 'iterator', 'lower_break', 'upper_break']
        collection = local_df.to_dict('index')

        # divide into pieces
        consolidation_price_ranges_df = pd.DataFrame()
        current_sub_df = pd.DataFrame()
        for key in collection:
            break_time = collection[key]['break_time']
            iterator = collection[key]['iterator']
            lower_bound = collection[key]['lower_break']
            upper_bound = collection[key]['upper_break']

            row_df = pd.DataFrame([[break_time, iterator, lower_bound, upper_bound]], columns=columns)

            if (iterator == 1) and (len(current_sub_df) != 0):
                if len(current_sub_df) == minimum_waves_count:
                    with_consolidation_price_ranges_df = self.get_price_ranges(current_sub_df)
                    consolidation_price_ranges_df = pd.concat(
                        [consolidation_price_ranges_df, with_consolidation_price_ranges_df],
                        axis=0
                    )

                current_sub_df = row_df
            else:
                current_sub_df = pd.concat([current_sub_df, row_df], axis=0)

            if len(current_sub_df) == minimum_waves_count:
                with_consolidation_price_ranges_df = self.get_price_ranges(current_sub_df)
                consolidation_price_ranges_df = pd.concat(
                    [consolidation_price_ranges_df, with_consolidation_price_ranges_df],
                    axis=0
                )

        return pd.merge(
            local_df[['break_time', 'market', 'iterator']], consolidation_price_ranges_df[
                ['break_time', 'iterator', 'consolidation_min', 'consolidation_max', 'consolidation_id']],
            how='left',
            on=['break_time', 'iterator']
        ) if not consolidation_price_ranges_df.empty else pd.DataFrame()

    def collect_consolidations(self,
                               dataframe: DataFrame,
                               allowed_percent_change: float,
                               waves_height_quantile: float,
                               minimum_waves_count: int) -> DataFrame:
        # consolidation with time ranges
        consolidations_df = self.get_consolidations_time_ranges(dataframe, allowed_percent_change, waves_height_quantile)
        # consolidation with price ranges
        return self.get_consolidations_price_ranges(consolidations_df, minimum_waves_count)

    # https://stackoverflow.com/a/62968313
    @staticmethod
    def initial_processing(dataframe: DataFrame) -> DataFrame:
        waves_filtered_df = dataframe[dataframe['market'].notnull()]
        selected_fields_df = waves_filtered_df[['market', 'break_time', 'break_value']].copy()
        selected_fields_df['previous_break_value'] = selected_fields_df['break_value'].shift(periods=1)
        ready_to_iterate_df = selected_fields_df[selected_fields_df['previous_break_value'].notnull()].copy()
        ready_to_iterate_df['height'] = abs(ready_to_iterate_df['break_value'] - ready_to_iterate_df['previous_break_value'])

        return ready_to_iterate_df

    def get_consolidations(self,
                           dataframe: DataFrame,
                           allowed_wave_percent_change: float,
                           waves_height_quantile: float,
                           minimum_waves_count: int) -> DataFrame:
        initially_processed_df = self.initial_processing(dataframe)
        with_consolidations_df = self.collect_consolidations(
            initially_processed_df,
            allowed_wave_percent_change,
            waves_height_quantile,
            minimum_waves_count
        )

        return pd.merge(
            dataframe, with_consolidations_df[['break_time', 'market', 'iterator', 'consolidation_min', 'consolidation_max', 'consolidation_id']],
            how='left',
            on=['break_time', 'market']
        ) if not with_consolidations_df.empty else pd.DataFrame()

    @staticmethod
    def get_open_position_signals(dataframe: DataFrame) -> DataFrame:
        # 1. pandas.core.base.DataError: No numeric types to aggregate, using numeric representation instead for expanding aggregation
        # 2. candle time is UTC based, converting to int (lambda) and back to timestamp with_last_consolidation_end introducing +2 factor: 2 * 60 * 60
        dataframe['recent_consolidation_end'] = dataframe['consolidation_id'] \
            .apply(lambda x: x.value / 1000000000 - (2 * 60 * 60) if x.value > 0 else 0) \
            .expanding(1) \
            .max()

        with_last_consolidation_end = dataframe[dataframe['recent_consolidation_end'] > 0].copy()
        with_last_consolidation_end['recent_consolidation_end'] = with_last_consolidation_end['recent_consolidation_end'] \
            .apply(lambda x: datetime.fromtimestamp(int(x)))

        aggregated = with_last_consolidation_end \
            .groupby(['recent_consolidation_end'])[['recent_consolidation_end', 'consolidation_min', 'consolidation_max']] \
            .agg(recent_consolidation_end=('recent_consolidation_end', 'max'),
                 recent_consolidation_min=('consolidation_min', 'max'),
                 recent_consolidation_max=('consolidation_max', 'max')
                 ) \
            .reset_index(drop=True)

        joined = pd.merge(
            with_last_consolidation_end, aggregated,
            how='left',
            on=['recent_consolidation_end']
        )

        joined['recent_consolidation_mid'] = \
            joined['recent_consolidation_max'] - (joined['recent_consolidation_max'] - joined['recent_consolidation_min']) / 2

        numpy_now = np.datetime64('now')

        joined['consolidation_break_at'] = np.where(
            joined['date_time'] > joined['recent_consolidation_end'],
            np.where(
                (joined['close'] > joined['recent_consolidation_max']) | (joined['close'] < joined['recent_consolidation_min']),
                joined['date_time'],
                numpy_now
            ),
            numpy_now
        )

        joined['position_side'] = np.where(
            joined['date_time'] > joined['recent_consolidation_end'],
            np.where(
                joined['close'] > joined['recent_consolidation_max'],
                'bullish',
                np.where(
                    joined['close'] < joined['recent_consolidation_min'],
                    'bearish',
                    np.nan
                )
            ),
            np.nan
        )

        consolidation_break_at = joined \
            .groupby(['recent_consolidation_end']) \
            .agg(open_position_at=pd.NamedAgg('consolidation_break_at', 'min')) \
            .reset_index(drop=True)

        with_open_position_signals = pd.merge(
            joined, consolidation_break_at,
            how='left',
            left_on='date_time',
            right_on='open_position_at'
        )

        return with_open_position_signals

    def analyze(self, dataframe: DataFrame) -> DataFrame:
        allowed_wave_percent_change = self.settings['allowed_wave_percent_change']
        waves_height_quantile = self.settings['waves_height_quantile']
        minimum_waves_count = self.settings['minimum_waves_count']
        consolidations = self.get_consolidations(dataframe, allowed_wave_percent_change, waves_height_quantile, minimum_waves_count)
        with_open_position_signals = self.get_open_position_signals(consolidations) \
            if not consolidations.empty \
            else pd.DataFrame()

        return with_open_position_signals

    def plot_with_consolidation_ranges(self, dataframe: DataFrame, figure: Figure) -> Figure:
        # return None
        filtered_df = dataframe[dataframe['consolidation_id'].notnull()]

        # https://www.shanelynn.ie/summarising-aggregation-and-grouping-data-in-python-pandas/
        aggregated_df = filtered_df.groupby('consolidation_id').agg(
            x0=pd.NamedAgg(column='break_time', aggfunc=min),
            x1=pd.NamedAgg(column='break_time', aggfunc=max),
            y0=pd.NamedAgg(column='consolidation_min', aggfunc=min),
            y1=pd.NamedAgg(column='consolidation_max', aggfunc=max)
        ).sort_values('x0')

        # https://colab.research.google.com/drive/1v9v4j1MnklaCd9eFcuGnB5x_5FoINmRe?usp=sharing
        # https://stackoverflow.com/a/64941705
        for index in aggregated_df.index:
            figure.add_trace(
                go.Scatter(
                    x=[aggregated_df['x0'][index], aggregated_df['x0'][index], aggregated_df['x1'][index], aggregated_df['x1'][index], aggregated_df['x0'][index]],
                    y=[aggregated_df['y0'][index], aggregated_df['y1'][index], aggregated_df['y1'][index], aggregated_df['y0'][index], aggregated_df['y0'][index]],
                    fill='toself'
                ),
                secondary_y=True
            )

        return figure

    def plot_chart(self, symbol, dataframe):
        # return None
        fig = make_subplots(rows=1, cols=1, shared_xaxes=True, specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Candlestick(x=dataframe["date_time"],
                                     open=dataframe["open"],
                                     high=dataframe["high"],
                                     low=dataframe["low"],
                                     close=dataframe["close"],
                                     name=symbol),
                      secondary_y=True)

        fig.update_layout(xaxis_rangeslider_visible=False)

        fig.add_trace(
            go.Scatter(
                x=dataframe["break_time"],
                y=dataframe["break_value"],
                mode='lines+markers',
                line=dict(color='black', width=2),
                connectgaps=True,
                name="waves"
            ),
            secondary_y=True
        )

        # consolidations
        fig = self.plot_with_consolidation_ranges(dataframe, fig)

        # fig.add_trace(go.Bar(
        #     x=dataframe['date_time'],
        #     y=dataframe['volume']),
        #     secondary_y=False)
        # fig.layout.yaxis2.showgrid = False

        fig.show()

    def run_scenario(self, dataframe, symbol, plot_func=None) -> DataFrame:
        result_df = self.analyze(dataframe)

        if (not result_df.empty) and (plot_func is not None):
            plot_func(symbol, result_df)

        return result_df
