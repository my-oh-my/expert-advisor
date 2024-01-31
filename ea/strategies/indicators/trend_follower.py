from itertools import groupby

import numpy as np
from pandas import DataFrame
from scipy.signal import argrelextrema


class TrendFollower:
    def __init__(self, settings):
        self.settings = settings

    def clean_extremes(self, collection: list):
        def process_group(group, extreme_type):
            if extreme_type == 'at_low':
                return min(group, key=lambda x: x['extreme_value'])
            else:
                return max(group, key=lambda x: x['extreme_value'])

        waves = [process_group(list(g), k) for k, g in groupby(collection, key=lambda x: x['side'])]

        return waves

    def get_extremes(self, dataframe: DataFrame, extreme_candles_range: int):
        # Find local extremes
        minima_idx_dirty = argrelextrema(dataframe['low'].values, comparator=np.less, order=extreme_candles_range)[0]
        minima_candidates = [
            dict(
                idx=idx,
                date_time=dataframe.iloc[idx]['date_time'],
                side='at_low',
                extreme_value=dataframe.iloc[idx]['low']
            ) for idx in minima_idx_dirty
        ]

        maxima_idx_dirty = \
        argrelextrema(dataframe['high'].values, comparator=np.greater_equal, order=extreme_candles_range)[0]
        maxima_candidates = [
            dict(
                idx=idx,
                date_time=dataframe.iloc[idx]['date_time'],
                side='at_high',
                extreme_value=dataframe.iloc[idx]['high']
            ) for idx in maxima_idx_dirty
        ]
        # Combine indices and sort them
        extremes_cleaned = self.clean_extremes(sorted(minima_candidates + maxima_candidates, key=lambda x: x['idx']))

        return extremes_cleaned

    def calculate_beta_coefficients(self, length, arr):
        x = np.arange(length)
        y = np.array(arr)

        # Calculate beta coefficient using polyfit
        beta, _ = np.polyfit(x, y, 1)

        return beta

    def get_betas(self, collection: list[dict], beta_extreme_count: int):
        # calculate beta coefficient of the consecutive extremes <current - n, current>
        beta_elements_count = beta_extreme_count
        extremes_indices = [element['idx'] for element in collection]
        extremes_sides = [element['side'] for element in collection]
        extremes_values = [element['extreme_value'] for element in collection]
        betas = [
            dict(
                idx=extremes_indices[i],
                side=extremes_sides[i],
                beta=self.calculate_beta_coefficients(
                    beta_elements_count,
                    extremes_values[i - beta_elements_count + 1:i + 1]
                )
            ) for i in range(beta_elements_count - 1, len(extremes_indices))
        ]

        return betas

    def get_market_analysis(self, extremes: list[dict], betas: list[dict]):
        # combine both collections whenever index matches
        # assign beta coefficient of the consecutive extremes
        market_analysis = [
            {**entry1, **entry2}
            for entry1 in extremes
            for entry2 in betas
            if (entry1['idx'] == entry2['idx']) & (entry1['side'] == entry2['side'])
        ]

        return market_analysis

    def with_rolling_attributes(self, dataframe: DataFrame, rolling_window: int):
        local_df = dataframe.copy()
        # Calculate n-time minima and maxima
        local_df['rolling_n_low'] = local_df['low'].rolling(window=rolling_window).min().shift(1)
        local_df['rolling_n_high'] = local_df['high'].rolling(window=rolling_window).max().shift(1)

        return local_df

    def with_beta_sign(self, dataframe: DataFrame, collection: list[dict]):
        local_df = dataframe.copy()
        # Assign beta
        market_analysis_indices = [element['idx'] for element in collection]
        market_analysis_extreme_datetimes = [element['date_time'] for element in collection]
        market_analysis_betas = [element['beta'] for element in collection]

        for i in range(len(market_analysis_indices) - 1):
            row_indexer = market_analysis_indices[i]
            # i + 1 in next iteration becomes i, so data is overwritten
            column_indexer = market_analysis_indices[i + 1]
            beta_coefficient = market_analysis_betas[i]
            # print(f'{row_indexer}: {column_indexer}')
            # sp500_data.loc[row_indexer: column_indexer, 'beta_sign_original'] = np.sign(beta_coefficient)
            #
            # modifier = rolling_window - 1
            # excluding local extreme itself
            modifier = 1
            local_df.loc[row_indexer + modifier: column_indexer, 'current_extreme_on'] = \
                market_analysis_extreme_datetimes[i]
            local_df.loc[row_indexer + modifier: column_indexer, 'beta_sign'] = np.sign(beta_coefficient)

        # Extra step for data after the most recent extreme
        recent_extreme_index = collection[-1]['idx']
        recent_beta = market_analysis_betas[-1]
        recent_extreme_on = collection[-1]['date_time']

        local_df.loc[recent_extreme_index:, 'current_extreme_on'] = recent_extreme_on
        local_df.loc[recent_extreme_index:, 'beta_sign'] = np.sign(recent_beta)

        return local_df

    def with_indicator(self, dataframe: DataFrame):
        local_df = dataframe.copy()
        # Assign n-time mins/max
        local_df.loc[
            (local_df['beta_sign'] == -1),
            # & (local_df['low'] > local_df['rolling_n_low'])
            'indicator'
        ] = local_df['rolling_n_low']
        local_df.loc[
            (local_df['beta_sign'] == 1),
            # & (local_df['high'] < local_df['rolling_n_high'])
            'indicator'
        ] = local_df['rolling_n_high']

        local_df['indicator_lead'] = local_df.shift(1)['indicator']

        return local_df

    def with_is_signal(self, dataframe: DataFrame) -> DataFrame:
        local_df = dataframe.copy()

        local_df.loc[
            (local_df['beta_sign'] == -1)
            & (local_df['low'] <= local_df['rolling_n_low'])
            & (local_df['indicator'] >= local_df['indicator_lead'])
            # & (local_df['date_time'] != local_df['current_extreme_on'])
            ,
            'is_signal'
        ] = local_df['rolling_n_low']
        local_df.loc[
            (local_df['beta_sign'] == 1)
            & (local_df['high'] >= local_df['rolling_n_high'])
            & (local_df['indicator'] <= local_df['indicator_lead'])
            # & (local_df['date_time'] != local_df['current_extreme_on'])
            ,
            'is_signal'
        ] = local_df['rolling_n_high']

        return local_df

    def analyze(self, dataframe: DataFrame) -> DataFrame:
        local_df = dataframe.copy()
        # parameters
        extreme_candles_range = int(self.settings['extreme_candles_range'])
        beta_extreme_count = int(self.settings['beta_extreme_count'])
        rolling_window = int(self.settings['rolling_window'])
        # transformations
        extremes = self.get_extremes(local_df, extreme_candles_range)

        betas = self.get_betas(extremes, beta_extreme_count)

        market_analysis = self.get_market_analysis(extremes, betas)

        with_applied_rolling_attributes = self.with_rolling_attributes(local_df, rolling_window)

        with_applied_beta_sign = self.with_beta_sign(with_applied_rolling_attributes, market_analysis)

        with_applied_indicator = self.with_indicator(with_applied_beta_sign)

        with_applied_is_signal = self.with_is_signal(with_applied_indicator)

        return with_applied_is_signal

    def plot_chart(self, symbol, dataframe):
        return None

    def run_scenario(self, dataframe, symbol, plot_func=None) -> DataFrame:
        result_df = self.analyze(dataframe)

        if (not result_df.empty) and (plot_func is not None):
            plot_func(symbol, result_df)

        return result_df
